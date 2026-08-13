package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"musget/internal/engine"
	"musget/internal/gallica"
)

var audioExts = map[string]bool{".mp3": true, ".flac": true, ".ogg": true, ".oga": true, ".wav": true, ".m4a": true, ".aac": true}
var scoreExts = map[string]bool{".pdf": true, ".djvu": true, ".mxl": true, ".mscz": true}
var videoExts = map[string]bool{".mp4": true, ".webm": true, ".mkv": true, ".ogv": true}

func cmdGet(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("get needs an identifier or ark")
	}
	id := args[0]
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	kind := fs.String("kind", "all", "audio|score|video|all")
	fileGlobs := fs.String("file", "", "comma-separated file globs")
	out := fs.String("out", "", "output dir (default ~/Music)")
	jobs := fs.Int("jobs", 0, "parallel downloads (0 = auto)")
	segments := fs.Int("segments", 0, "split big files into N parallel ranges (0=off)")
	segMin := fs.Int64("segment-min", 25<<20, "min bytes for segmented download")
	verify := fs.Bool("verify", true, "verify checksums")
	proxy := fs.String("proxy", "", "proxy URL")
	relay := fs.String("relay", "", "force CORS relay (URL or cors.sh/eu.org)")
	install := fs.Bool("install", false, "organize under ~/Music/<Identifier>/")
	quiet := fs.Bool("q", false, "quiet")
	// remember which flags the user actually set (for defaults)
	segMinSet := false
	segmentsSet := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "segment-min":
			segMinSet = true
		case "segments":
			segmentsSet = true
		}
	})
	fs.Parse(args[1:])

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	if out == nil || *out == "" {
		home, _ := os.UserHomeDir()
		*out = filepath.Join(home, "Music")
	}

	// gallica ark?
	if isArk(id) {
		return getGallica(ctx, id, *out, *proxy, *quiet)
	}

	p := *proxy
	_ = p
	relayFlag = *relay
	ac, mode, bases, err := pickArchiveClient(ctx)
	if err != nil {
		return err
	}
	if mode == modeCORS {
		ac.Fetch = curlFetch(bases)
	}
	it, err := ac.Item(ctx, id)
	if err != nil {
		return err
	}
	if it.Title != "" && !*quiet {
		fmt.Printf("item: %s — %s (transport: %s)\n", it.Identifier, it.Title, mode)
	}

	var globs []*regexp.Regexp
	if *fileGlobs != "" {
		for _, g := range strings.Split(*fileGlobs, ",") {
			g = strings.TrimSpace(g)
			if g == "" {
				continue
			}
			re, err := regexp.Compile("(?i)" + globToRegex(g))
			if err != nil {
				return fmt.Errorf("bad glob %q: %w", g, err)
			}
			globs = append(globs, re)
		}
	}

	baseDir := *out
	if *install {
		baseDir = filepath.Join(*out, sanitize(it.Identifier))
	}

	var jobsList []engine.Job
	for _, f := range it.Files {
		ln := strings.ToLower(f.Name)
		if f.Format == "Metadata" || strings.Contains(ln, "sample") ||
			strings.Contains(ln, "_text.pdf") || strings.Contains(ln, "text_djvu") ||
			strings.HasSuffix(ln, ".vbr") {
			continue
		}
		ext := strings.ToLower(filepath.Ext(f.Name))
		if !matchKind(ext, *kind) {
			continue
		}
		// for scores prefer pdf over djvu when both exist
		if *kind == "score" && ext == ".djvu" {
			continue
		}
		if len(globs) > 0 {
			matched := false
			for _, re := range globs {
				if re.MatchString(f.Name) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if f.Size <= 0 {
			continue
		}
		name := filepath.Base(f.Name)
		dest := filepath.Join(baseDir, sanitize(name))
		jobsList = append(jobsList, engine.Job{
			Name: name,
			URL:  ac.DownloadURL(it.Identifier, f.Name),
			Dest: dest,
			Size: f.Size,
			MD5:  f.MD5,
			SHA1: f.SHA1,
		})
	}
	if len(jobsList) == 0 {
		return fmt.Errorf("no matching files to download")
	}
	if !*quiet {
		fmt.Printf("%d files, %s\n", len(jobsList), humanSize(totalSize(jobsList)))
	}
	useJobs := *jobs
	if useJobs <= 0 {
		useJobs = 8
		if mode == modeCORS {
			useJobs = 16
		}
	}
	eng := &engine.Engine{
		Jobs:       useJobs,
		Segments:   *segments,
		SegmentMin: *segMin,
		Verify:     *verify,
		Quiet:      *quiet,
		HTTP:       ac.HTTP,
		MaxTries:   4,
	}
	// CORS relay handles parallel ranges well; default to segmented for big
	// files unless the user chose otherwise. Transfers go through curl because
	// the relay's Cloudflare rejects Go's TLS fingerprint.
	if mode == modeCORS {
		eng.Streamer = curlStreamer(bases)
		if !segmentsSet {
			eng.Segments = 4
			if !segMinSet {
				// benchmark mp3s (2-10MB) never hit the 25MB default, so
				// moderately large files get segmented through the relay.
				eng.SegmentMin = 8 << 20
			}
		}
	}
	st := eng.Run(ctx, jobsList)
	if st.FilesBad > 0 {
		for _, l := range st.Lines {
			fmt.Fprintln(os.Stderr, l)
		}
		return fmt.Errorf("%d file(s) failed", st.FilesBad)
	}
	return nil
}

func getGallica(ctx context.Context, ark, out, proxy string, quiet bool) error {
	gc := gallica.NewClient(proxy)
	dest := filepath.Join(out, "gallica-"+ark+".pdf")
	if !quiet {
		fmt.Printf("gallica: downloading %s\n", ark)
	}
	start := time.Now()
	if err := gc.Download(ctx, ark, dest); err != nil {
		return err
	}
	if !quiet {
		fmt.Printf("ok %s (%s, %s)\n", dest, humanSize(fileSize(dest)), time.Since(start).Round(time.Millisecond))
	}
	return nil
}

func isArk(s string) bool {
	return regexp.MustCompile(`^[a-z0-9]{6,14}$`).MatchString(s) &&
		(strings.HasPrefix(s, "bpt6k") || strings.HasPrefix(s, "btv1b") || strings.HasPrefix(s, "bpt6t"))
}

func matchKind(ext, kind string) bool {
	switch kind {
	case "audio":
		return audioExts[ext]
	case "score":
		return scoreExts[ext]
	case "video":
		return videoExts[ext]
	default:
		return audioExts[ext] || scoreExts[ext] || videoExts[ext]
	}
}

func globToRegex(g string) string {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range g {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String()
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		}
		return r
	}, s)
}

func totalSize(jobs []engine.Job) int64 {
	var t int64
	for _, j := range jobs {
		t += j.Size
	}
	return t
}

func fileSize(p string) int64 {
	if fi, err := os.Stat(p); err == nil {
		return fi.Size()
	}
	return 0
}
