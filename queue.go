package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"time"

	//"github.com/Akegarasu/blivedm-go/message"
	_ "github.com/mattn/go-sqlite3"

	log "github.com/sirupsen/logrus"
)

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func makeDir(path string) error {
	if !pathExists(path) {
		return os.MkdirAll(path, os.ModePerm)
	}
	return nil
}

func UserHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

type Queue struct {
	mu        sync.RWMutex
	roomID    string
	db        *sql.DB
	giftQueue chan *QueueUser
	done      chan struct{}
	cache     sync.Map // 用于缓存用户信息
}

type SyncMessage struct {
	Cmd  string     `json:"cmd"`
	Data []LiveUser `json:"data"`
}

type QueueUser struct {
	Uid        int
	Uname      string
	GuardLevel int
	Gifts      int
	Now        int
}

type LiveUser struct {
	Uid        string `json:"uid"`
	Nickname   string `json:"nickname"`
	GuardLevel string `json:"level"`
	Gifts      string `json:"gifts"`
	Now        string `json:"now"`
}

func NewLiveUser(u *QueueUser) LiveUser {
	return LiveUser{
		Uid:        i2s(u.Uid),
		Nickname:   u.Uname,
		GuardLevel: i2s(u.GuardLevel),
		Gifts:      i2s(u.Gifts),
		Now:        i2s(u.Now),
	}
}

func (b LiveUser) Json() string {
	marshal, err := json.Marshal(b)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func NewQueue(roomID string) *Queue {
	if er := makeDir(UserHomeDir() + "\\AppData\\Roaming\\blive-queue"); er != nil {
		log.Error("创建目录失败: ", er)
		// Don't return nil, return an empty Queue
	}
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		log.Error("打开数据库失败: ", err)
		// Don't return nil, return an empty Queue
	}

	// 生产友好的数据库优化配置
	db.Exec("PRAGMA synchronous = NORMAL")     // 平衡性能和安全性
	db.Exec("PRAGMA journal_mode = WAL")       // WAL模式，适合并发
	db.Exec("PRAGMA temp_store = MEMORY")      // 临时表存储在内存
	db.Exec("PRAGMA cache_size = 5000")        // 适中的缓存大小
	db.SetMaxOpenConns(4)                      // 设置合理的连接数

	db.Exec(`CREATE TABLE IF NOT EXISTS queue (
        uid INTEGER PRIMARY KEY,
        nickname TEXT,
        level INTEGER,
        gifts INTEGER,
        topped INTEGER DEFAULT 0,
		now INTEGER DEFAULT 0,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );`)
	db.Exec(`CREATE TABLE IF NOT EXISTS totalGifts (
        uid INTEGER PRIMARY KEY,
        nickname TEXT,
        level INTEGER,
        gifts INTEGER
    );`)
	db.Exec(`CREATE TABLE IF NOT EXISTS lastCutin (
        uid INTEGER PRIMARY KEY
    );`)
	
	// 创建必要的索引以提高查询性能
	db.Exec("CREATE INDEX IF NOT EXISTS idx_queue_gifts ON queue(gifts)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_queue_timestamp ON queue(timestamp)")
	
	q := &Queue{
		roomID:    roomID,
		db:        db,
		giftQueue: make(chan *QueueUser, 2000), // 生产友好：缓冲2000个gift消息
		done:      make(chan struct{}),
	}
	
	// 启动2个异步处理goroutine（生产友好配置）
	go q.processGiftQueue(0)
	go q.processGiftQueue(1)
	
	return q
}

// Close 关闭队列和数据库连接
func (q *Queue) Close() {
	close(q.done)
	if q.db != nil {
		q.db.Close()
	}
}

// AddGiftAsync 异步添加gift消息
func (q *Queue) AddGiftAsync(u *QueueUser) {
	select {
	case q.giftQueue <- u:
		// 成功加入队列
	default:
		// 队列满了，记录警告但不阻塞
		log.Warn("Gift queue is full, dropping gift message")
	}
}

// processGiftQueue 异步处理gift队列（生产友好版本）
func (q *Queue) processGiftQueue(workerID int) {
	batch := make([]*QueueUser, 0, 15) // 生产友好：批量处理15个
	ticker := time.NewTicker(100 * time.Millisecond) // 保持100ms处理间隔
	defer ticker.Stop()
	
	for {
		select {
		case gift := <-q.giftQueue:
			batch = append(batch, gift)
			if len(batch) >= 15 { // 生产友好：15个一批
				q.processBatch(batch)
				batch = batch[:0] // 重置切片
			}
		case <-ticker.C:
			if len(batch) > 0 {
				q.processBatch(batch)
				batch = batch[:0]
			}
		case <-q.done:
			// 处理剩余的消息
			if len(batch) > 0 {
				q.processBatch(batch)
			}
			return
		}
	}
}

// processBatch 批量处理gift消息
func (q *Queue) processBatch(gifts []*QueueUser) {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	tx, err := q.db.Begin()
	if err != nil {
		log.Error("开始事务失败: ", err)
		return
	}
	defer tx.Rollback()
	
	for _, gift := range gifts {
		q.processSingleGift(tx, gift)
	}
	
	if err := tx.Commit(); err != nil {
		log.Error("提交事务失败: ", err)
	}
}

// processSingleGift 处理单个gift消息（在事务中）
func (q *Queue) processSingleGift(tx *sql.Tx, u *QueueUser) {
	if q.isNowTx(tx, u.Uid) {
		return
	}

	// 先检查缓存
	if cached, ok := q.cache.Load(u.Uid); ok {
		cachedUser := cached.(*QueueUser)
		// 更新缓存
		cachedUser.Gifts += u.Gifts
		q.cache.Store(u.Uid, cachedUser)
	}

	rows, err := tx.Query("SELECT level, gifts FROM queue WHERE uid = ?", u.Uid)
	if err != nil {
		return
	}
	defer rows.Close()

	gf := u.Gifts
	if rows.Next() {
		var level, gifts int
		if err := rows.Scan(&level, &gifts); err != nil {
			return
		}
		if u.GuardLevel > 80 {
			if level < u.GuardLevel {
				_, err = tx.Exec("UPDATE queue SET level = ?, timestamp = CURRENT_TIMESTAMP WHERE uid = ?", u.GuardLevel, u.Uid)
			}
		} else if gf > 0 {
			gf = gifts + u.Gifts
			_, err = tx.Exec("UPDATE queue SET gifts = ? WHERE uid = ?", gf, u.Uid)
		}
	} else {
		if u.GuardLevel > 80 {
			gf = 0
		}
		var gl int
		switch u.GuardLevel {
		case 1:
			gl = 3
		case 3:
			gl = 1
		default:
			gl = u.GuardLevel
		}
		gf_old := 0
		err = tx.QueryRow("SELECT gifts FROM totalGifts WHERE uid = ?", u.Uid).Scan(&gf_old)
		if err != nil {
			gf_old = 0
		}
		gf += gf_old
		_, err = tx.Exec("INSERT INTO queue (uid, nickname, level, gifts) VALUES (?, ?, ?, ?)", u.Uid, u.Uname, gl, gf)
		if err == nil {
			tx.Exec("DELETE FROM totalGifts WHERE uid = ?", u.Uid)
		}
	}
	
	if q.isCutinTx(tx, u.Uid) {
		_, err = tx.Exec("UPDATE queue SET topped = 65536 WHERE uid = ?", u.Uid)
		if err == nil {
			tx.Exec("DELETE FROM lastCutin WHERE uid = ?", u.Uid)
		}
	}
}

func (q *Queue) Add(u *QueueUser) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q._Add(u)
}

func (q *Queue) _Add(u *QueueUser) bool {
	if q.isNow(u.Uid) {
		return false
	}

	rows, err := q.db.Query("SELECT level, gifts FROM queue WHERE uid = ?", u.Uid)
	if err != nil {
		return false
	}
	defer rows.Close()

	gf := u.Gifts
	if rows.Next() {
		var level, gifts int
		if err := rows.Scan(&level, &gifts); err != nil {
			return false
		}
		err = sql.ErrNoRows
		if u.GuardLevel > 80 {
			if level < u.GuardLevel {
				_, err = q.db.Exec("UPDATE queue SET level = ?, timestamp = CURRENT_TIMESTAMP WHERE uid = ?", u.GuardLevel, u.Uid)
				if err != nil {
					return false
				}
			}
		} else if gf > 0 {
			gf = gifts + u.Gifts
			_, err = q.db.Exec("UPDATE queue SET gifts = ? WHERE uid = ?", gf, u.Uid)
			if err != nil {
				return false
			}
		}
	} else {
		if u.GuardLevel > 80 {
			gf = 0
		}
		var gl int
		switch u.GuardLevel {
		case 1:
			gl = 3
		case 3:
			gl = 1
		default:
			gl = u.GuardLevel
		}
		gf_old := 0
		err = q.db.QueryRow("SELECT gifts FROM totalGifts WHERE uid = ?", u.Uid).Scan(&gf_old)
		if err != nil {
			gf_old = 0
		}
		gf += gf_old
		_, err = q.db.Exec("INSERT INTO queue (uid, nickname, level, gifts) VALUES (?, ?, ?, ?)", u.Uid, u.Uname, gl, gf)
		if err != nil {
			return false
		}
		q.db.Exec("DELETE FROM totalGifts WHERE uid = ?", u.Uid)
	}
	if q.isCutin(u.Uid) {
		_, err = q.db.Exec("UPDATE queue SET topped = 65536 WHERE uid = ?", u.Uid)
		if err != nil {
			return false
		}
		q.db.Exec("DELETE FROM lastCutin WHERE uid = ?", u.Uid)
	}
	return err == nil
}

// isNowTx 在事务中检查用户是否正在进行
func (q *Queue) isNowTx(tx *sql.Tx, uid int) bool {
	exists := 0
	err := tx.QueryRow("SELECT 1 FROM queue WHERE uid = ? AND now = 1", uid).Scan(&exists)
	if err != nil {
		return false
	}
	return exists > 0
}

// isCutinTx 在事务中检查是否为插队用户
func (q *Queue) isCutinTx(tx *sql.Tx, uid int) bool {
	var exists int
	err := tx.QueryRow("SELECT 1 FROM lastCutin WHERE uid = ?", uid).Scan(&exists)
	if err != nil {
		return false
	}
	return exists > 0
}

func (q *Queue) Remove(uid int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	_, err := q.db.Exec("DELETE FROM queue WHERE uid = ?", uid)
	return err == nil
}

func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	q.db.Exec("DELETE FROM queue")
	q.cache = sync.Map{} // 清空缓存
}

func (q *Queue) ClearTotalGifts() {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	q.db.Exec("DELETE FROM totalGifts")
}

func (q *Queue) Top(uid int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q._Top(uid)
}

func (q *Queue) _Top(uid int) bool {
	var topped int
	err := q.db.QueryRow("SELECT topped FROM queue WHERE now = 0 AND topped < 65535 ORDER BY topped DESC LIMIT 1").Scan(&topped)
	if err != nil {
		return false
	}
	_, err = q.db.Exec("UPDATE queue SET topped = ? WHERE now = 0 AND uid = ?", topped+1, uid)
	return err == nil
}

func (q *Queue) FetchOrderedQueue() ([]*QueueUser, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	
	users := make(map[int]QueueUser)
	var uidList []int

	querySet := [...]string{
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 1",                                                                                      // 0. NOW
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped > 0 ORDER BY topped DESC, level DESC, gifts DESC, timestamp ASC",           // 1. Topped users
		//"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND level > 0 ORDER BY level DESC, timestamp ASC",                      // 2. New & old abos
		//"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND level = 0 AND gifts >= 52 ORDER BY gifts DESC, timestamp ASC",      // 3. Cut-ins
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND timestamp > 0 ORDER BY timestamp ASC", // 4. All users
	}

	for i := 0; i < len(querySet); i++ {
		rows, err := q.db.Query(querySet[i])
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var uid, level, gifts, now int
			var nickname string
			if err := rows.Scan(&uid, &nickname, &level, &gifts, &now); err != nil {
				continue
			}
			users[uid] = QueueUser{
				Uid:        uid,
				Uname:      nickname,
				GuardLevel: level,
				Gifts:      gifts,
				Now:        now,
			}
			uidList = append(uidList, uid)
		}
	}

	var result []*QueueUser
	for _, uid := range uidList {
		user := users[uid]
		result = append(result, &user)
	}
	return result, nil
}

func (q *Queue) FetchFirstUser() *QueueUser {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q._FetchUser(0)
}

func (q *Queue) _FetchUser(pos int) *QueueUser {
	if pos < 0 {
		return nil
	}
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+q.roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		return nil
	}
	defer db.Close()

	var uid, level, gifts, now int
	var nickname string
	flag := false
	_cnt := 0

	querySet := [...]string{
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 1",                                                                                      // 0. NOW
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped > 0 ORDER BY topped DESC, level DESC, gifts DESC, timestamp ASC",           // 1. Topped users
		//"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND level > 0 ORDER BY level DESC, timestamp ASC",                      // 2. New & old abos
		//"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND level = 0 AND gifts >= 52 ORDER BY gifts DESC, timestamp ASC",      // 3. Cut-ins
		"SELECT uid, nickname, level, gifts, now FROM queue WHERE now = 0 AND topped = 0 AND timestamp > 0 ORDER BY timestamp ASC", // 4. All users
	}

	for i := 0; i < len(querySet); i++ {
		rows, err := db.Query(querySet[i])
		if err != nil {
			return nil
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&uid, &nickname, &level, &gifts, &now); err != nil {
				continue
			}
			if _cnt < pos {
				_cnt++
				continue
			} else {
				flag = true
				break
			}
		}
		if flag {
			break
		}
	}

	if flag {
		return &QueueUser{
			Uid:        uid,
			Uname:      nickname,
			GuardLevel: level,
			Gifts:      gifts,
			Now:        now,
		}
	}

	return nil
}

func (q *Queue) UpdateGifts(u *QueueUser) bool {
	// 现在主要使用异步处理，这个方法可以简化
	q.AddGiftAsync(u)
	return true
}

func (q *Queue) In(u *QueueUser) bool {
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+q.roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		return false
	}
	defer db.Close()

	var exists int
	err = db.QueryRow("SELECT 1 FROM queue WHERE uid = ?", u.Uid).Scan(&exists)
	return err == nil
}

func (q *Queue) Encode() *SyncMessage {
	users, err := q.FetchOrderedQueue()
	if err != nil {
		return &SyncMessage{Cmd: "SYNC", Data: []LiveUser{}}
	}
	d := make([]LiveUser, 0, len(users))
	for _, user := range users {
		d = append(d, NewLiveUser(user))
	}
	return &SyncMessage{
		Cmd:  "SYNC",
		Data: d,
	}
}

func (q *Queue) formCutin() {
	return
	q.mu.Lock()
	defer q.mu.Unlock()
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+q.roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT uid FROM queue WHERE now = 0 AND gifts >= 52")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var uid int
		if err := rows.Scan(&uid); err != nil {
			continue
		}
		_, err = db.Exec("INSERT INTO lastCutin (uid) VALUES (?)", uid)
		if err != nil {
			continue
		}
	}
}

func (q *Queue) formCutinAndClearQueue() {
	return
	q.formCutin()
	q.Clear()
}

func (q *Queue) clearCutin() {
	return
	q.mu.Lock()
	defer q.mu.Unlock()
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+q.roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		return
	}
	defer db.Close()

	db.Exec("DELETE FROM lastCutin")
}

func (q *Queue) isCutin(uid int) bool {
	return false
	db, err := sql.Open("sqlite3", "file:"+UserHomeDir()+"\\AppData\\Roaming\\blive-queue\\queue_"+q.roomID+".db?cache=shared&mode=rwc")
	if err != nil {
		return false
	}
	defer db.Close()

	var exists int
	err = db.QueryRow("SELECT 1 FROM lastCutin WHERE uid = ?", uid).Scan(&exists)
	if err != nil {
		return false
	}
	return exists > 0
}

func (q *Queue) isNow(uid int) bool {
	exists := 0
	err := q.db.QueryRow("SELECT 1 FROM queue WHERE uid = ? AND now = 1", uid).Scan(&exists)
	if err != nil {
		return false
	}
	return exists > 0
}

func (q *Queue) Start(uid int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q._Start(uid)
}

func (q *Queue) _Start(uid int) bool {
	_, err := q.db.Exec("DELETE FROM queue WHERE now = 1")
	if err != nil {
		return false
	}
	_, err = q.db.Exec("UPDATE queue SET now = 1 WHERE uid = ?", uid)
	return err == nil
}

func (q *Queue) NextUser() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	u := q._FetchUser(0)
	if u != nil {
		if u.Now == 1 {
			u = q._FetchUser(1)
			if u == nil {
				return false
			}
		}
		return q._Start(u.Uid)
	}
	return false
}
