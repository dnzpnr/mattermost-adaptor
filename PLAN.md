# mattermost-adapter — PLAN

Kaynak: MindAlert'in çalışan Mattermost köprüsü (`/home/dp/MindAlert`, SALT-OKUNUR).
MindAlert'te hiçbir dosya değiştirilmez. Kod okunur, bu repoya İngilizce
tanımlayıcılarla uyarlanarak taşınır. MindAlert'i bu çekirdeğe bağlamak kapsam DIŞI.

## Kaynak → hedef eşlemesi

| Kaynak (MindAlert) | Hedef | Değişiklik |
|---|---|---|
| `tenant_resolver/mattermost.py: kanal_ve_thread_cikar` | `normalize.extract_channel_and_thread` | aynı mantık |
| `mattermost_bot/run.py: surucu_kur, _pozitif_sonlu_env` | `config.AdapterConfig.from_env` + `transport.build_driver` | yapılandırma ayrıldı; env adları KORUNUR |
| `mattermost_bot/run.py: TekBaglantiWebsocket` | `transport.SingleConnectionWebsocket` | print → logging(stderr) |
| `mattermost_bot/run.py: kopru_dongusunu_calistir, _kalici_kimlik_hatasi` | `transport.run_with_reconnect`, `transport.is_permanent_auth_error` | köprü fabrikası parametre |
| `bridge/relay.py: LibreChatKoprusu` (kuyruk, tek worker, hello bariyeri, drain, replay, tekillik, filtreler, `_cevapla`) | `listener.MattermostListener` | dal mantığı (LibreChat/MCP/ingestion) ÇIKARILDI → `MessageHandler` |
| `bridge/relay.py: KopruPostDurumu` + `yonetim/db.py` kopru_* tabloları | `state.StateStore` + `state.SqliteStateStore` | aynı SQL semantiği, İngilizce tablo adları |
| `tenant_resolver/resolver.py: TenantCozucu` | `session.SessionResolver` protokolü + `session.ThreadSessionResolver` | tenant kavramı adaptörde yok |
| `mattermost_bot/relay_bridge.py: MattermostDriverDosyaIstemcisi` | `transport.MattermostTransport.get_file_*` | + dosya adı temizleme |

Taşınmayanlar: `bridge/http_client.py`, `provisioning`, `agent_provisioning`, `mcp_client`,
`file_ingest` (hafıza/ingestion kısmı), `trace.py`/`trace_export.py`, `celiski/*`, YonetimDB.

## Paket yapısı (Python ≥3.11, bağımlılıklar: mattermostdriver==7.3.2, websockets==17.1)

```
pyproject.toml            console script: mattermost-adapter = mattermost_adapter.cli:main
src/mattermost_adapter/
  __init__.py             public API re-export
  __main__.py             python -m mattermost_adapter
  errors.py               AdapterError(code, message, exit_code) alt sınıfları
  config.py               AdapterConfig (frozen), from_env(environ); repr token'ı maskeler
  logging.py              stderr JSON-satır logger; redact(); correlation_id
  models.py               NormalizedMessage, Sender, Attachment, ConversationRef, Session,
                          OutboundMessage, SentMessage, FileInfo, NormalizationResult (to_dict)
  normalize.py            parse_event, normalize_post, extract_channel_and_thread (SAF, ağ yok)
  session.py              SessionResolver protokolü, ThreadSessionResolver
  state.py                StateStore protokolü, SqliteStateStore
  transport.py            build_driver, SingleConnectionWebsocket, MattermostTransport,
                          is_permanent_auth_error, run_with_reconnect
  listener.py             MattermostListener, MessageHandler protokolü
  api.py                  MattermostAdapter (uygulama katmanı)
  cli.py                  argparse; YALNIZ api.py'yi çağırır
tests/acceptance/         Opus'un yazdığı kabul testleri (DEĞİŞTİRİLMEZ)
tests/ported/             MindAlert testlerinden taşınan karakterizasyon testleri (Sol)
```

## Sözleşmeler

### NormalizedMessage (`to_dict()` alan sırası sabit)
```json
{
  "provider": "mattermost",
  "channel_id": "c1",
  "thread_id": "p1",
  "message_id": "p2",
  "parent_message_id": "p1",
  "sender": {"id": "u1", "display_name": "@ayse"},
  "content": "merhaba",
  "attachments": [{"id": "f1", "name": "a.pdf", "size": 12, "mime_type": "application/pdf"}],
  "timestamp": "2026-09-14T10:00:00.000Z",
  "raw_metadata": {"post_type": "", "create_at": 1789380000000, "channel_type": "O",
                   "team_id": "t1", "props": {}}
}
```
- `thread_id = post.root_id or post.id`; `parent_message_id = post.root_id or null`.
- `sender.display_name`: websocket `data.sender_name` varsa o, yoksa `null`.
- `attachments`: `post.file_ids` sırası; ad/boyut/mime `post.metadata.files` içinde aynı id varsa doldurulur, yoksa `null`.
- `timestamp`: `create_at` (ms) → UTC ISO-8601, milisaniyeli, `Z` son eki. create_at yoksa `null`.
- `raw_metadata`: yalnız yukarıdaki anahtarlar (izin listesi); eksik olan `null` (props → `{}`).

### Oturum
- `ConversationRef(provider, channel_id, thread_id)`.
- `SessionResolver.resolve(ref) -> Session | None`; `None` = konuşma reddedildi (eski "eşlenmemiş kanal": sessiz, handler çağrılmaz, post işlenmiş SAYILMAZ).
- `ThreadSessionResolver`: `Session(session_id=f"mattermost:{thread_id}", attributes={})`.
- `user_id` oturum anahtarına ASLA girmez.

### normalize-event sonucu (`NormalizationResult.to_dict()`)
```json
{"action": "process" | "ignore", "ignore_reason": null | "not_posted_event" | "own_message"
   | "system_message" | "empty_message" | "unmapped_conversation",
 "message": {...} | null, "session": {"session_id": "...", "attributes": {}} | null}
```
Girdi: ham websocket olayı (`{"event":"posted","data":{"post":"<json string>",...}}`, `data.post` dict de kabul) ya da çıplak Post nesnesi (`id` + `channel_id` içeren dict). `--bot-user-id` verilirse kendi mesajı elenir. Filtre sırası kaynaktaki `_postu_isle` ile aynı: own → system_ → (resolve) unmapped → boş (dosya yok ve `message.strip()` boş) → process. `message` ignore durumunda da (post çözülebildiyse) doldurulur.

### CLI zarfı (stdout'a TEK JSON nesnesi, `listen` hariç)
```json
{"status": "success", "data": {...}, "error": null}
{"status": "error", "data": null, "error": {"code": "CONFIG_ERROR", "message": "..."}}
```
Çıkış kodları: 0 başarı · 1 INTERNAL_ERROR · 2 INVALID_INPUT/USAGE_ERROR · 3 CONFIG_ERROR · 4 AUTH_ERROR · 5 MATTERMOST_API_ERROR (ağ/HTTP ≥400, 401/403 hariç).
Loglar yalnız stderr. Hiçbir çıktıda (stdout/stderr) MM_TOKEN değeri, `Authorization` başlığı ya da `Bearer <...>` geçmez.

### Komutlar
| Komut | Girdi | data | Config |
|---|---|---|---|
| `normalize-event [--bot-user-id ID]` | stdin JSON | NormalizationResult | hayır |
| `send-message` | stdin `{"channel_id", "thread_id"?, "text", "reply_to_message_id"?, "props"?}` | `{"message_id","channel_id","thread_id","create_at"}` | evet |
| `fetch-file --file-id ID --output-dir DIR` | — | `{"file_id","name","size","mime_type","path"}` | evet |
| `health [--check-connection]` | — | `{"config":{"url","port","scheme","token_present"},"connection":null|{"ok","bot_user_id","username"}}` | `--check-connection` yoksa eksik config hata DEĞİL (`token_present:false`) |
| `listen` | — | stdout NDJSON: YALNIZ `action=process` sonuçları, her satır `{"type":"message","correlation_id":"...","data":NormalizationResult}` (ignore edilenler yalnız stderr log); ölümcül hata son satır `{"type":"error","error":{code,message}}` + ilgili çıkış kodu (config 3, kalıcı auth 4) | evet |

`send-message`: `create_post({"channel_id", "message": text, "root_id": thread_id or "", "props": props ∪ {reply_prop_key: reply_to_message_id}})`. `reply_prop_key` = `MM_ADAPTER_REPLY_PROP_KEY` (varsayılan `reply_to_post_id`); `reply_to_message_id` yoksa eklenmez. `text` boş/yalnız boşluk → INVALID_INPUT.

`fetch-file`: dosya adı `os.path.basename` + boşsa `file_id`; yol `output-dir` dışına çıkamaz; aynı ad varsa üzerine yazılmaz, `name (1).ext` biçimi.

`listen`: `MattermostListener` + `SqliteStateStore(MM_ADAPTER_STATE_PATH)`; env verilmezse varsayılan yol `${XDG_STATE_HOME:-~/.local/state}/mattermost-adapter/state.db` (dizin oluşturulur). Programatik API'de `state_store=None` verilirse replay/tekillik KAPALI (kaynaktaki `post_durumu is None` davranışı); CLI her zaman SQLite kullanır + replay kanalları `MM_ADAPTER_CHANNELS` (virgüllü). Handler satırı yazıp flush ettikten sonra post işlenmiş sayılır. SIGTERM/SIGINT → intake durur, drain, çıkış 0.

### Config (env adları kaynağa uyumlu)
`MM_URL`(localhost) `MM_PORT`(8065) `MM_SCHEME`(http) `MM_TOKEN`(zorunlu, strip) `MM_REQUEST_TIMEOUT_SECONDS`(30) `MM_WEBSOCKET_RECV_GAP_THRESHOLD_SECONDS`(60) `MM_EVENT_LOOP_LAG_THRESHOLD_SECONDS`(1) `MM_EVENT_LOOP_WATCHDOG_HEARTBEAT_SECONDS`(300) `MM_BRIDGE_QUEUE_CAPACITY`(50, 0<x<100) `MM_BRIDGE_DRAIN_DEADLINE_SECONDS`(180) `MM_ADAPTER_STATE_PATH` `MM_ADAPTER_CHANNELS` `MM_ADAPTER_REPLY_PROP_KEY` `MM_ADAPTER_LOG_LEVEL`(INFO).

### Listener davranışı (kaynakla BİREBİR)
hello bariyeri → replay → canlı kuyruk; sınırlı kuyruk + backpressure; tek worker; drain bitmeden reconnect yok; replay kanal başına en yeni 100, taşmada uyarı; ilk cursor websocket öncesi; tekillik; own/system/empty işlenmiş sayılır, unmapped sayılmaz; post işlenmiş kaydı handler BAŞARIYLA döndükten SONRA (at-least-once); handler istisnası worker'ı düşürür ve özgün istisna yükselir. Handler sync ise `asyncio.to_thread`, coroutine ise await.

### Gözlemlenebilirlik
Her post işleme için `correlation_id` (uuid4). stderr log satırı JSON: `{"ts","level","event","provider":"mattermost","channel_id","thread_id","message_id","session_id","correlation_id",...}`. Kaynaktaki log noktaları (post alındı, dal başladı/bitti + sure_ms, REST süreleri, replay uyarıları, watchdog) aynı `event` adlarıyla korunur.

## Adımlar
1. İskelet + config + errors + logging + models + normalize + session (saf katman) → kabul testleri 1-4, 6, 9.
2. state + listener + transport → taşınan relay/run testleri.
3. api + cli → kabul testleri 5, 7, 8, güvenlik.
4. README (komut referansı, örnek JSON, plugin çağırma örneği, teknik borç).
Her adım sonunda `pytest` tamamı yeşil.

## Kararlar (kullanıcı, 2026-09-14)
- Oturum çözücü takılabilir; varsayılan `mattermost:<thread_id>`.
- `listen` durumu takılabilir StateStore, varsayılan SQLite dosyası.
- Tanımlayıcılar İngilizce.
- MindAlert'e dokunulmaz; legacy bağlama kapsam dışı.

## Plan eleştirisi (Sol, 1 tur) ve kararlar
1. State varsayılanı çelişkiliydi → KABUL: CLI'de varsayılan XDG yolu (yukarıda).
2. Reply prop varsayılanı `mindalert_cevap_post_id` olmalı → RED: kaynakta proje adı olmayacak (kabul testi `test_source_has_no_librechat_or_runtime_coupling`); eski ad `MM_ADAPTER_REPLY_PROP_KEY` ile verilebilir (kabul testi `test_reply_prop_key_is_configurable_and_caller_props_are_kept`). İSTİSNA: `test_bridge_relay.py:1330-1352` taşınırken reply prop anahtarı testte açıkça `mindalert_cevap_post_id` verilir; iddia değişmez.
3. Kabul testleri yoktu → GEÇERSİZ: `tests/acceptance/` eleştiri sırasında yazıldı.

## Taşınan testler için istisna kuralı
MindAlert testleri `tests/ported/` altına taşınır. Davranış iddiaları (hangi post işlenir/elenir, sıra, tekillik, cursor, backpressure, drain, reconnect, create_post gövdesi) AYNEN korunur. Değişebilecekler yalnız: isimler (İngilizce), LibreChat/MCP/ingestion dalları yerine sahte `MessageHandler`, `print`/capsys yerine stderr JSON log iddiası (aynı olay adı + kimlik alanları), tenant çözücü yerine sahte `SessionResolver`. LibreChat/MCP/çelişki/ingestion/trace'e özgü testler TAŞINMAZ; taşınmayanlar `tests/ported/PORTING.md` içinde gerekçesiyle listelenir.
