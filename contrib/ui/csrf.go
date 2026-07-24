package ui

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

const csrfTTL = time.Hour

func issueCSRF(secret []byte, instanceID string, now time.Time) string {
	exp := now.Add(csrfTTL).Unix()
	mac := csrfMAC(secret, instanceID, exp)
	payload := strconv.FormatInt(exp, 10) + "|" + base64.RawURLEncoding.EncodeToString(mac)
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func verifyCSRF(secret []byte, instanceID, token string, now time.Time) bool {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return false
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 2 {
		return false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || now.Unix() > exp {
		return false
	}
	mac, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	want := csrfMAC(secret, instanceID, exp)
	return hmac.Equal(mac, want)
}

func csrfMAC(secret []byte, instanceID string, exp int64) []byte {
	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(exp))
	m := hmac.New(sha256.New, secret)
	_, _ = m.Write([]byte(instanceID))
	_, _ = m.Write([]byte{'\n'})
	_, _ = m.Write(expBuf[:])
	return m.Sum(nil)
}
