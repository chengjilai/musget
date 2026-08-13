package main

// post-loop: videocrawl-style automatic "download PD music + repost to
// bilibili" loop.
//
// Law gate: only recordings with a verified publication year >50 years old
// (China's sound-recording term) are posted; items without a date are
// rejected with a reason. Redundancy: state file dedups by archive.org item
// id (queued/downloaded/posted/failed/rejected), and --check-bili compares
// against the account's existing video titles before posting.
//
// State: ~/.musget/repost-state.jsonl (one JSON object per line, append +
// rewrite, human-reviewable). Log: ~/.musget/post-loop.log.

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"musget/pkg/archivex"
	"musget/pkg/engine"
)

type PostItem struct {
	ID      string `json:"id"`
	Title   string `json:"title,omitempty"` // optional override
	Source  string `json:"source,omitempty"`
	Year    int    `json:"year,omitempty"` // verified publication year
	Status  string `json:"status"`         // queued|downloaded|posted|failed|rejected
	Reason  string `json:"reason,omitempty"`
	BVID    string `json:"bvid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Updated string `json:"updated"`
}

type postState struct {
	items map[string]*PostItem
	path  string
}

func loadState(path string) (*postState, error) {
	os.MkdirAll(filepath.Dir(path), 0o755)
	st := &postState{items: map[string]*PostItem{}, path: path}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var it PostItem
		if err := json.Unmarshal([]byte(line), &it); err != nil {
			continue
		}
		st.items[it.ID] = &it
	}
	return st, nil
}

func (s *postState) save() error {
	ids := make([]string, 0, len(s.items))
	for id := range s.items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var b strings.Builder
	for _, id := range ids {
		it := s.items[id]
		it.Updated = time.Now().Format(time.RFC3339)
		j, _ := json.Marshal(it)
		b.Write(j)
		b.WriteByte('\n')
	}
	return os.WriteFile(s.path, []byte(b.String()), 0o644)
}

var (
	reYear = regexp.MustCompile(`^(19\d\d|18\d\d)`)
	reBvid = regexp.MustCompile(`BV[0-9A-Za-z]{10}`)
)

// pdYear extracts a publication year from archive.org metadata (date/year
// fields, first 4-digit year found). Returns 0 when unverifiable.
func pdYear(m map[string]any) int {
	for _, k := range []string{"date", "year"} {
		v, ok := m[k]
		if !ok {
			continue
		}
		switch t := v.(type) {
		case string:
			if m := reYear.FindString(t); m != "" {
				if n, err := strconv.Atoi(m); err == nil {
					return n
				}
			}
		case []any:
			for _, e := range t {
				if s, ok := e.(string); ok {
					if m := reYear.FindString(s); m != "" {
						if n, err := strconv.Atoi(m); err == nil {
							return n
						}
					}
				}
			}
		}
	}
	return 0
}

func (s *postState) enqueue(seed string) error {
	id, title := seed, ""
	if i := strings.Index(seed, ":"); i > 0 {
		id, title = seed[:i], seed[i+1:]
	}
	if id == "" {
		return fmt.Errorf("empty seed")
	}
	if _, exists := s.items[id]; exists {
		return nil // already tracked
	}
	s.items[id] = &PostItem{ID: id, Title: title, Status: "queued"}
	return nil
}

// existingBiliTitles lists the account's published video titles (best-effort;
// empty on API failure).
func existingBiliTitles() map[string]bool {
	out := map[string]bool{}
	sess, err := os.ReadFile("/run/secrets/bili-upload-web.json")
	if err != nil {
		sess, err = os.ReadFile(os.ExpandEnv("$HOME/.config/bili-web-session.json"))
	}
	if err != nil {
		return out
	}
	var js struct {
		Cookie string `json:"cookie"`
	}
	if json.Unmarshal(sess, &js) != nil || js.Cookie == "" {
		return out
	}
	req, err := newReq("https://member.bilibili.com/x/web/archives?pn=1&ps=50&status=all", js.Cookie)
	if err != nil {
		return out
	}
	body, err := doReq(req)
	if err != nil {
		return out
	}
	var d struct {
		Data struct {
			Archives []struct {
				Title string `json:"title"`
			} `json:"archives"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &d) != nil {
		return out
	}
	for _, a := range d.Data.Archives {
		out[normalizeTitle(a.Title)] = true
	}
	return out
}

func normalizeTitle(t string) string {
	return strings.ToLower(strings.Join(strings.Fields(t), " "))
}

// buildVideo concatenates the item's mp3s (in order) and muxes them with a
// title card into an h264 mp4 at outPath.
func buildVideo(ctx context.Context, audio []string, outPath, line1, line2, line3 string) error {
	if len(audio) == 0 {
		return fmt.Errorf("no audio files")
	}
	var list strings.Builder
	for _, a := range audio {
		list.WriteString(fmt.Sprintf("file '%s'\n", strings.ReplaceAll(a, "'", `'\''`)))
	}
	concatIn := outPath + ".concat"
	os.WriteFile(concatIn, []byte(list.String()), 0o644)
	defer os.Remove(concatIn)
	concatMp3 := outPath + ".mp3"
	defer os.Remove(concatMp3)
	if len(audio) == 1 {
		concatMp3 = audio[0]
	} else {
		if err := runCmd(ctx, "ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", concatIn, "-c", "copy", concatMp3); err != nil {
			return err
		}
	}
	font := findFont()
	card := outPath + ".png"
	defer os.Remove(card)
	vf := fmt.Sprintf(
		"drawtext=fontfile=%s:text='%s':fontcolor=white:fontsize=50:x=(w-text_w)/2:y=250,"+
			"drawtext=fontfile=%s:text='%s':fontcolor=0xf2c14e:fontsize=38:x=(w-text_w)/2:y=340,"+
			"drawtext=fontfile=%s:text='%s':fontcolor=0xaaaaaa:fontsize=26:x=(w-text_w)/2:y=410",
		font, escDraw(truncStr(line1, 60)), font, escDraw(truncStr(line2, 70)), font, escDraw(truncStr(line3, 70)))
	if err := runCmd(ctx, "ffmpeg", "-y", "-v", "quiet", "-f", "lavfi", "-i", "color=c=0x14161a:s=1280x720:d=10", "-vf", vf, "-frames:v", "1", card); err != nil {
		return err
	}
	return runCmd(ctx, "ffmpeg", "-y", "-v", "error", "-loop", "1", "-i", card, "-i", concatMp3,
		"-vf", "scale=1280:720", "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p",
		"-af", "loudnorm=I=-16:TP=-1.5:LRA=11", "-c:a", "aac", "-b:a", "192k", "-shortest", "-movflags", "+faststart", outPath)
}

func escDraw(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "’")
	s = strings.ReplaceAll(s, ":", "：")
	s = strings.ReplaceAll(s, ",", "，")
	return s
}

func findFont() string {
	cands := []string{
		"/nix/store/dm5cigvarwb6h9kl9q0yjasjyllksrfk-gyre-fonts-2.501/share/fonts/opentype/texgyreheros-bold.otf",
	}
	for _, c := range cands {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func runCmd(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %v (%s)", name, err, truncStr(string(out), 200))
	}
	return nil
}

// postUpload uploads via ~/src/bilibili/upload_web.py and returns the bvid.
func postUpload(ctx context.Context, video, title, source, tags, desc string) (string, error) {
	script := os.ExpandEnv("$HOME/src/bilibili/upload_web.py")
	if _, err := os.Stat(script); err != nil {
		return "", fmt.Errorf("upload_web.py missing: %w", err)
	}
	cmd := exec.CommandContext(ctx, "python3", script, video,
		"--title", title, "--source", source, "--tag", tags, "--tid", "3", "--desc", desc)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("upload: %v (%s)", err, truncStr(string(out), 300))
	}
	m := reBvid.FindString(string(out))
	if m == "" {
		return "", fmt.Errorf("no bvid in upload output: %s", truncStr(string(out), 200))
	}
	return m, nil
}

// buildDesc assembles the attribution description (law + attribution pattern).
func buildDesc(creator, work string, year int, label, source string) string {
	labelS := ""
	if label != "" {
		labelS = " (" + label + ")"
	}
	return fmt.Sprintf(`%s，公有领域 (Public Domain) 历史录音 — 录音超过50年，可自由分享。

作曲/演奏: %s
录音年份: %d%s
来源: archive.org — %s`,
		truncStr(work, 100), creator, year, labelS, source)
}

func metaStr(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, ", ")
	default:
		return fmt.Sprint(v)
	}
}

func newReq(u, cookie string) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", cookie)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	return req, nil
}

func doReq(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}

func cmdPostLoop(args []string) error {
	fs := flag.NewFlagSet("post-loop", flag.ExitOnError)
	every := fs.Int("every", 0, "loop interval seconds (0 = one round)")
	limit := fs.Int("limit", 3, "max uploads per round")
	dryRun := fs.Bool("dry-run", false, "decide only, no download/upload")
	seed := fs.String("seed", "", "comma-separated item ids, optional :CustomTitle")
	checkBili := fs.Bool("check-bili", false, "skip titles already on the account")
	noUpload := fs.Bool("no-upload", false, "download+encode only (upload from another machine)")
	uploadOnly := fs.Bool("upload-only", false, "upload items already staged as videos (state+Videos/Post synced)")
	state := fs.String("state", os.ExpandEnv("$HOME/.musget/repost-state.jsonl"), "state file")
	quiet := fs.Bool("q", false, "quiet")
	fs.Parse(args)

	st, err := loadState(*state)
	if err != nil {
		return err
	}
	if *seed != "" {
		for _, s := range strings.Split(*seed, ",") {
			if s = strings.TrimSpace(s); s != "" {
				st.enqueue(s)
			}
		}
		st.save()
	}
	logf := func(format string, a ...any) {
		msg := fmt.Sprintf(format, a...)
		line := time.Now().Format("15:04:05") + " " + msg
		fmt.Fprintln(os.Stderr, line)
		os.MkdirAll(os.ExpandEnv("$HOME/.musget"), 0o755)
		if f, err := os.OpenFile(os.ExpandEnv("$HOME/.musget/post-loop.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			f.WriteString(line + "\n")
			f.Close()
		}
	}

	existing := map[string]bool{}
	if *checkBili {
		existing = existingBiliTitles()
		logf("[check-bili] %d existing titles on the account", len(existing))
	}

	if *uploadOnly {
		uctx := context.Background()
		n := 0
		for _, it := range st.items {
			if it.Status != "downloaded" || it.BVID != "" {
				continue
			}
			video := filepath.Join(os.ExpandEnv("$HOME/Videos/Post"), it.ID+".mp4")
			if _, err := os.Stat(video); err != nil {
				logf("    %s: staged video missing: %v", it.ID, err)
				continue
			}
			title := it.Title
			if title == "" {
				title = it.ID
			}
			tags := "古典音乐,历史录音," + it.ID
			bvid, err := postUpload(uctx, video, title, "https://archive.org/details/"+it.ID, tags,
				fmt.Sprintf("公有领域 (Public Domain) 历史录音 — 录音超过50年。\n来源: https://archive.org/details/%s", it.ID))
			if err != nil {
				it.Reason = "upload: " + err.Error()
				logf("    FAIL upload %s: %v", it.ID, err)
				continue
			}
			it.Status, it.BVID, it.Reason = "posted", bvid, ""
			logf("    POSTED %s → https://www.bilibili.com/video/%s", title, bvid)
			n++
			st.save()
		}
		logf("[upload-only] posted %d", n)
		return nil
	}

	ctx := context.Background()
	for round := 0; ; round++ {
		if round > 0 && *every <= 0 {
			break
		}
		// pick queued candidates, oldest first
		var cands []*PostItem
		for _, it := range st.items {
			if it.Status == "queued" || it.Status == "downloaded" {
				cands = append(cands, it)
			}
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i].Updated < cands[j].Updated })
		if len(cands) == 0 {
			logf("[round %d] no queued candidates", round)
			if *every <= 0 {
				break
			}
			time.Sleep(time.Duration(*every) * time.Second)
			continue
		}
		logf("[round %d] %d queued, posting up to %d", round, len(cands), *limit)
		posted := 0
		for _, it := range cands {
			if posted >= *limit {
				break
			}
			logf("--- %s (%s)", it.ID, it.Title)
			// 1. law gate: verify PD via metadata
			ac, mode, bases, speeds, err := pickArchiveClient(ctx)
			if err != nil {
				logf("    transport: %v", err)
				continue
			}
			if mode == modeCORS {
				ac.Fetch = curlFetch(bases, speeds)
			}
			item, err := ac.Item(ctx, it.ID)
			if err != nil {
				it.Status, it.Reason = "queued", "transient: "+err.Error()
				logf("    TRANSIENT item fetch failed, requeued: %v", err)
				st.save()
				continue
			}
			if item.Title == "" && len(item.Files) == 0 {
				it.Status, it.Reason = "queued", "transient: empty metadata (egress)"
				logf("    TRANSIENT empty metadata (relay egress), requeued")
				st.save()
				continue
			}
			if mt := item.Mediatype; mt != "" && mt != "audio" {
				it.Status, it.Reason = "rejected", "mediatype="+mt
				logf("    REJECT non-audio (%s)", mt)
				st.save()
				continue
			}
			year := pdYear(item.Metadata)
			if year == 0 {
				it.Status, it.Reason = "rejected", "no publication date in metadata"
				logf("    REJECT no-date (law gate)")
				st.save()
				continue
			}
			if year > time.Now().Year()-51 {
				it.Status, it.Reason = "rejected", fmt.Sprintf("recorded %d (<51y ago)", year)
				logf("    REJECT too recent: %d (law gate)", year)
				st.save()
				continue
			}
			it.Year = year
			// 2. redundancy: already posted locally or on bilibili
			if it.BVID != "" {
				it.Status, it.Reason = "posted", "bvid "+it.BVID
				logf("    already posted %s", it.BVID)
				st.save()
				continue
			}
			title := it.Title
			if title == "" {
				title = truncStr(item.Title, 60)
			}
			if existing[normalizeTitle(title)] {
				it.Status, it.Reason = "rejected", "title already on bilibili"
				logf("    REJECT duplicate title on bilibili")
				st.save()
				continue
			}
			logf("    PD ok: %d → post \"%s\"", year, title)
			if *dryRun {
				it.Status = "queued"
				continue
			}
			// 3. download all mp3s
			workDir := filepath.Join(os.TempDir(), "musget-post", it.ID)
			os.RemoveAll(workDir)
			os.MkdirAll(workDir, 0o755)
			var audio []string
			for _, f := range item.Files {
				ln := strings.ToLower(f.Name)
				if !strings.HasSuffix(ln, ".mp3") || strings.Contains(ln, "sample") {
					continue
				}
				audio = append(audio, f.Name)
			}
			if len(audio) == 0 {
				it.Status, it.Reason = "failed", "no mp3 files"
				logf("    FAIL no mp3s")
				st.save()
				continue
			}
			sort.Strings(audio)
			var jobs []engine.Job
			for _, n := range audio {
				jobs = append(jobs, engine.Job{Name: n, URL: ac.DownloadURL(it.ID, n), Dest: filepath.Join(workDir, sanitize(filepath.Base(n))), Size: 0})
			}
			eng := &engine.Engine{Jobs: 8, Verify: false, Quiet: *quiet, HTTP: ac.HTTP, MaxTries: 3}
			if mode == modeCORS {
				eng.Streamer = curlStreamer(bases, speeds)
				eng.Segments = 4
				eng.SegmentMin = 15 << 20
			}
			res := eng.Run(ctx, jobs)
			if res.FilesBad.Load() > 0 {
				it.Status, it.Reason = "failed", fmt.Sprintf("%d files failed", res.FilesBad)
				logf("    FAIL %d/%d files", res.FilesBad, len(jobs))
				st.save()
				continue
			}
			var audioPaths []string
			for _, j := range jobs {
				audioPaths = append(audioPaths, j.Dest)
			}
			it.Status = "downloaded"
			st.save()
			// 4. build video
			creator := metaStr(item.Metadata, "creator")
			if creator == "" {
				creator = truncStr(item.Title, 40)
			}
			out := filepath.Join(os.ExpandEnv("$HOME/Videos/Post"), it.ID+".mp4")
			os.MkdirAll(filepath.Dir(out), 0o755)
			if err := buildVideo(ctx, audioPaths, out, creator, title, fmt.Sprintf("recorded %d%s", year, labelSuffix(item))); err != nil {
				it.Status, it.Reason = "failed", "video: "+err.Error()
				logf("    FAIL video: %v", err)
				st.save()
				continue
			}
			// 5. upload (skipped in --no-upload mode; upload elsewhere with --upload-only)
			if *noUpload {
				logf("    STAGED %s → %s (upload later with --upload-only)", title, out)
				st.save()
				continue
			}
			tags := "古典音乐,历史录音"
			for _, w := range strings.Fields(strings.NewReplacer(".", " ", ",", " ", "&", " ").Replace(creator)) {
				if len(tags) > 0 && len(tags) < 60 && !strings.Contains(tags, w) {
					tags += "," + w
				}
			}
			bvid, err := postUpload(ctx, out, title, "https://archive.org/details/"+it.ID, tags, buildDesc(creator, item.Title, year, labelName(item), "https://archive.org/details/"+it.ID))
			if err != nil {
				it.Status, it.Reason = "failed", "upload: " + err.Error()
				logf("    FAIL upload: %v", err)
				st.save()
				continue
			}
			it.Status, it.BVID, it.Reason = "posted", bvid, ""
			logf("    POSTED %s → https://www.bilibili.com/video/%s", title, bvid)
			posted++
			st.save()
		}
		if *every <= 0 {
			break
		}
		time.Sleep(time.Duration(*every) * time.Second)
	}
	return nil
}

func labelName(it *archivex.Item) string {
	if l, ok := it.Metadata["publisher"].(string); ok && l != "" {
		return l
	}
	return ""
}

func labelSuffix(it *archivex.Item) string {
	if l := labelName(it); l != "" {
		return " (" + l + ")"
	}
	return ""
}

func cmdPostStatus(args []string) error {
	fs := flag.NewFlagSet("post-status", flag.ExitOnError)
	state := fs.String("state", os.ExpandEnv("$HOME/.musget/repost-state.jsonl"), "state file")
	fs.Parse(args)
	st, err := loadState(*state)
	if err != nil {
		return err
	}
	var items []*PostItem
	for _, it := range st.items {
		items = append(items, it)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	fmt.Printf("%-52s %-10s %-6s %-14s %s\n", "ID", "STATUS", "YEAR", "BVID", "REASON")
	for _, it := range items {
		fmt.Printf("%-52s %-10s %-6d %-14s %s\n", truncStr(it.ID, 52), it.Status, it.Year, it.BVID, truncStr(it.Reason, 40))
	}
	return nil
}

func cmdPostSeed(args []string) error {
	fs := flag.NewFlagSet("post-seed", flag.ExitOnError)
	state := fs.String("state", os.ExpandEnv("$HOME/.musget/repost-state.jsonl"), "state file")
	fs.Parse(args)
	if fs.NArg() == 0 {
		return fmt.Errorf("post-seed ID[:Title] [more...]")
	}
	st, err := loadState(*state)
	if err != nil {
		return err
	}
	for _, a := range fs.Args() {
		st.enqueue(a)
	}
	return st.save()
}
