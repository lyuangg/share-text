package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"sync"
	"syscall"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

//go:embed index.html
var indexHTML []byte

const historyMax = 10

var slotNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,24}$`)

func validSlotName(name string) bool {
	return slotNameRe.MatchString(name)
}

type slot struct {
	text    string
	version int64
	history []string
	cur     int
}

type store struct {
	mu         sync.RWMutex
	data       map[string]*slot
	registered []string
	subs       map[chan []byte]struct{}
	saveMu     sync.Mutex
	savePath   string
	saveTimer  *time.Timer
}

type updateMsg struct {
	Slot     string `json:"slot"`
	Text     string `json:"text"`
	Version  int64  `json:"version"`
	ClientID string `json:"clientId"`
}

func newStore(savePath string) *store {
	return &store{
		data:       map[string]*slot{"top": {}, "bottom": {}},
		registered: []string{"top", "bottom"},
		subs:       make(map[chan []byte]struct{}),
		savePath:   savePath,
	}
}

type diskSlot struct {
	Text    string   `json:"text"`
	Version int64    `json:"version"`
	History []string `json:"history,omitempty"`
	Cur     int      `json:"cur,omitempty"`
}

type diskData struct {
	Slots map[string]diskSlot `json:"slots"`
	Order []string            `json:"order,omitempty"`
}

func (s *store) load() {
	b, err := os.ReadFile(s.savePath)
	if err != nil {
		return
	}
	var d diskData
	if err := json.Unmarshal(b, &d); err != nil {
		log.Printf("读取历史文件失败: %v", err)
		return
	}
	s.mu.Lock()
	order := d.Order
	for _, name := range order {
		if !validSlotName(name) {
			continue
		}
		if _, ok := s.data[name]; !ok {
			s.data[name] = &slot{history: []string{}, cur: -1}
		}
	}
	for name := range d.Slots {
		if validSlotName(name) {
			if _, ok := s.data[name]; !ok {
				s.data[name] = &slot{history: []string{}, cur: -1}
			}
		}
	}
	for name, ds := range d.Slots {
		if !validSlotName(name) {
			continue
		}
		cur := -1
		text := ds.Text
		if len(ds.History) > 0 {
			if ds.Cur < 0 || ds.Cur >= len(ds.History) {
				ds.Cur = len(ds.History) - 1
			}
			cur = ds.Cur
			text = ds.History[cur]
		}
		s.data[name] = &slot{
			text:    text,
			version: ds.Version,
			history: ds.History,
			cur:     cur,
		}
	}
	// 校准注册顺序：已注册但缺失的补到最后
	seen := map[string]bool{}
	var merged []string
	for _, n := range order {
		if validSlotName(n) && !seen[n] {
			seen[n] = true
			merged = append(merged, n)
		}
	}
	for n := range s.data {
		if !seen[n] {
			merged = append(merged, n)
		}
	}
	s.registered = merged
	s.mu.Unlock()
	log.Printf("已加载历史数据: %s", s.savePath)
}

func (s *store) scheduleSave() {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if s.saveTimer != nil {
		s.saveTimer.Stop()
	}
	s.saveTimer = time.AfterFunc(500*time.Millisecond, func() { s.save() })
}

func (s *store) save() {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	s.mu.RLock()
	d := diskData{Slots: make(map[string]diskSlot, len(s.data)), Order: append([]string(nil), s.registered...)}
	for name, sl := range s.data {
		d.Slots[name] = diskSlot{
			Text:    sl.text,
			Version: sl.version,
			History: sl.history,
			Cur:     sl.cur,
		}
	}
	s.mu.RUnlock()

	b, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return
	}
	tmp := s.savePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		log.Printf("保存历史失败: %v", err)
		return
	}
	if err := os.Rename(tmp, s.savePath); err != nil {
		log.Printf("保存历史失败: %v", err)
	}
}

func (s *store) get(name string) (string, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sl := s.data[name]
	if sl == nil {
		return "", 0
	}
	return sl.text, sl.version
}

func (s *store) set(name, text, clientID string) updateMsg {
	s.mu.Lock()
	sl := s.ensure(name)
	if text == sl.text {
		msg := updateMsg{Slot: name, Text: text, Version: sl.version, ClientID: clientID}
		s.mu.Unlock()
		return msg
	}
	if len(sl.history) == 0 {
		sl.history = []string{text}
		sl.cur = 0
	} else {
		sl.history = sl.history[:sl.cur+1]
		sl.history = append(sl.history, text)
		if len(sl.history) > historyMax {
			drop := len(sl.history) - historyMax
			sl.history = sl.history[drop:]
			sl.cur -= drop
		}
		sl.cur = len(sl.history) - 1
	}
	sl.text = text
	sl.version++
	msg := updateMsg{Slot: name, Text: text, Version: sl.version, ClientID: clientID}
	s.pushLocked(msg)
	s.mu.Unlock()
	s.scheduleSave()
	return msg
}

func (s *store) undo(name, clientID string) (updateMsg, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sl := s.ensure(name)
	if sl.cur <= 0 {
		return updateMsg{}, false
	}
	sl.cur--
	sl.text = sl.history[sl.cur]
	sl.version++
	msg := updateMsg{Slot: name, Text: sl.text, Version: sl.version, ClientID: clientID}
	s.pushLocked(msg)
	s.scheduleSave()
	return msg, true
}

func (s *store) redo(name, clientID string) (updateMsg, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sl := s.ensure(name)
	if sl.cur >= len(sl.history)-1 {
		return updateMsg{}, false
	}
	sl.cur++
	sl.text = sl.history[sl.cur]
	sl.version++
	msg := updateMsg{Slot: name, Text: sl.text, Version: sl.version, ClientID: clientID}
	s.pushLocked(msg)
	s.scheduleSave()
	return msg, true
}

func (s *store) history(name string) ([]string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sl := s.data[name]
	if sl == nil {
		return nil, -1
	}
	out := make([]string, len(sl.history))
	copy(out, sl.history)
	return out, sl.cur
}

func (s *store) ensure(name string) *slot {
	sl := s.data[name]
	if sl == nil {
		sl = &slot{history: []string{}, cur: -1}
		s.data[name] = sl
	}
	return sl
}

func (s *store) addSlot(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validSlotName(name) {
		return false
	}
	if _, ok := s.data[name]; ok {
		return false
	}
	s.data[name] = &slot{history: []string{}, cur: -1}
	s.registered = append(s.registered, name)
	s.pushSlotLocked("add", name)
	s.scheduleSave()
	return true
}

func (s *store) removeSlot(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validSlotName(name) {
		return false
	}
	if _, ok := s.data[name]; !ok {
		return false
	}
	if len(s.registered) == 0 || name == s.registered[0] {
		return false
	}
	delete(s.data, name)
	for i, n := range s.registered {
		if n == name {
			s.registered = append(s.registered[:i], s.registered[i+1:]...)
			break
		}
	}
	s.pushSlotLocked("remove", name)
	s.scheduleSave()
	return true
}

func (s *store) slots() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.registered...)
}

func (s *store) pushSlotLocked(action, name string) {
	data, _ := json.Marshal(map[string]any{"type": "slot", "action": action, "slot": name})
	for ch := range s.subs {
		select {
		case ch <- data:
		default:
		}
	}
}

func (s *store) pushLocked(msg updateMsg) {
	data, _ := json.Marshal(msg)
	for ch := range s.subs {
		select {
		case ch <- data:
		default:
		}
	}
}

func (s *store) subscribe() chan []byte {
	ch := make(chan []byte, 16)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *store) unsubscribe(ch chan []byte) {
	s.mu.Lock()
	delete(s.subs, ch)
	s.mu.Unlock()
}

func main() {
	port := flag.Int("port", 0, "listen port (default: random free port)")
	noOpen := flag.Bool("no-open", false, "don't open browser automatically")
	dataPath := flag.String("data", "share-text-data.json", "history data file")
	flag.Parse()

	if *port == 0 {
		ln0, err := net.Listen("tcp4", ":0")
		if err != nil {
			log.Fatalf("无法分配端口: %v", err)
		}
		*port = ln0.Addr().(*net.TCPAddr).Port
		ln0.Close()
	}

	st := newStore(*dataPath)
	st.load()
	lanIP := detectLANIP()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("/api/info", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"lanUrl": fmt.Sprintf("http://%s:%d/", lanIP, *port),
			"ip":     lanIP,
			"port":   *port,
		})
	})

	mux.HandleFunc("/api/slots", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, map[string]any{"slots": st.slots()})
		case http.MethodPost, http.MethodDelete:
			var in struct {
				Slot     string `json:"slot"`
				ClientID string `json:"clientId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if r.Method == http.MethodPost {
				if !st.addSlot(in.Slot) {
					writeJSON(w, map[string]any{"ok": false, "error": "invalid or exists"})
					return
				}
				writeJSON(w, map[string]any{"ok": true})
			} else {
				if !st.removeSlot(in.Slot) {
					writeJSON(w, map[string]any{"ok": false, "error": "not found"})
					return
				}
				writeJSON(w, map[string]any{"ok": true})
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		slotName := r.URL.Query().Get("slot")
		if slotName == "" {
			slotName = r.URL.Path
		}
		switch r.Method {
		case http.MethodPost:
			var in updateMsg
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if !validSlotName(in.Slot) {
				http.Error(w, "invalid slot", http.StatusBadRequest)
				return
			}
			writeJSON(w, st.set(in.Slot, in.Text, in.ClientID))
		case http.MethodGet, http.MethodDelete:
			if !validSlotName(slotName) {
				http.Error(w, "missing slot", http.StatusBadRequest)
				return
			}
			if r.Method == http.MethodDelete {
				st.removeSlot(slotName)
				writeJSON(w, map[string]any{"ok": true})
				return
			}
			text, version := st.get(slotName)
			if r.URL.Query().Get("hist") == "1" {
				hist, cur := st.history(slotName)
				writeJSON(w, map[string]any{
					"slot":    slotName,
					"text":    text,
					"version": version,
					"cur":     cur,
					"history": hist,
				})
			} else {
				writeJSON(w, updateMsg{Slot: slotName, Text: text, Version: version})
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	for _, ep := range []string{"/api/undo", "/api/redo"} {
		func(ep string) {
			mux.HandleFunc(ep, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				var in updateMsg
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				if !validSlotName(in.Slot) {
					http.Error(w, "invalid slot", http.StatusBadRequest)
					return
				}
				var msg updateMsg
				var ok bool
				if ep == "/api/undo" {
					msg, ok = st.undo(in.Slot, in.ClientID)
				} else {
					msg, ok = st.redo(in.Slot, in.ClientID)
				}
				if ok {
					writeJSON(w, msg)
				} else {
					writeJSON(w, map[string]any{"error": ep, "slot": in.Slot})
				}
			})
		}(ep)
	}

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := st.subscribe()
		defer st.unsubscribe(ch)

		for _, name := range st.slots() {
			data, _ := json.Marshal(map[string]any{"type": "slot", "action": "add", "slot": name})
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		for _, name := range st.slots() {
			text, version := st.get(name)
			data, _ := json.Marshal(updateMsg{Slot: name, Text: text, Version: version, ClientID: "server"})
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case data := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	})

	mux.HandleFunc("/qr.png", func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.Query().Get("url")
		if url == "" {
			http.Error(w, "missing url", http.StatusBadRequest)
			return
		}
		png, err := qrcode.Encode(url, qrcode.Medium, 512)
		if err != nil {
			http.Error(w, "qr error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(png)
	})

	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("端口 %d 无法监听（可能已被其他程序占用）：%v\n请换一个端口，例如：go run . -port 8080", *port, err)
	}

	log.Printf("share-text running:")
	log.Printf("  local : http://localhost:%d/", *port)
	log.Printf("  LAN   : http://%s:%d/", lanIP, *port)

	if !*noOpen {
		go func() {
			time.Sleep(300 * time.Millisecond)
			openBrowser(fmt.Sprintf("http://localhost:%d/", *port))
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		st.save()
		log.Printf("正在退出，历史已保存到 %s", *dataPath)
		os.Exit(0)
	}()

	if err := http.Serve(ln, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(v)
}

func detectLANIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	fallback := ""
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP.To4()
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			if ip.IsPrivate() {
				return ip.String()
			}
			if fallback == "" {
				fallback = ip.String()
			}
		}
	}
	if fallback != "" {
		return fallback
	}
	return "127.0.0.1"
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		log.Printf("打开浏览器失败: %v", err)
	}
}