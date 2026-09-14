# mattermost-adapter

`mattermost-adapter`, Mattermost mesajlarını thread-temelli oturumlara dönüştüren bağımsız bir taşıma katmanıdır. Gelen websocket postlarını NDJSON olarak yayınlar; mesaj gönderme, dosya indirme, normalizasyon ve sağlık kontrolü için tek seferlik CLI komutları sunar. Kullanıcı kimliği oturum anahtarına katılmaz: aynı thread içindeki bütün kullanıcılar aynı `session_id` değerini paylaşır.

## Kurulum

Python 3.11 veya daha yenisi gerekir. Bu depodaki geliştirme ortamı şöyle hazırlanabilir:

```sh
python3.12 -m venv .venv
.venv/bin/pip install -e .
```

Temel bağlantı değişkenleri `MM_URL`, `MM_PORT`, `MM_SCHEME` ve `MM_TOKEN`'dır. `MM_TOKEN` ağ kullanan komutlarda zorunludur. Ek ayarlar:

- `MM_REQUEST_TIMEOUT_SECONDS` (varsayılan `30`)
- `MM_WEBSOCKET_RECV_GAP_THRESHOLD_SECONDS` (varsayılan `60`)
- `MM_EVENT_LOOP_LAG_THRESHOLD_SECONDS` (varsayılan `1`)
- `MM_EVENT_LOOP_WATCHDOG_HEARTBEAT_SECONDS` (varsayılan `300`)
- `MM_BRIDGE_QUEUE_CAPACITY` (varsayılan `50`, `0 < value < 100`)
- `MM_BRIDGE_DRAIN_DEADLINE_SECONDS` (varsayılan `180`)
- `MM_ADAPTER_STATE_PATH` (varsayılan `${XDG_STATE_HOME:-~/.local/state}/mattermost-adapter/state.db`)
- `MM_ADAPTER_CHANNELS` (replay yapılacak virgülle ayrılmış kanal kimlikleri)
- `MM_ADAPTER_REPLY_PROP_KEY` (varsayılan `reply_to_post_id`)
- `MM_ADAPTER_LOG_LEVEL` (varsayılan `INFO`)

## Dizin yapısı

```text
src/mattermost_adapter/
  api.py          uygulama katmanı
  cli.py          argparse ve JSON/NDJSON zarfı
  config.py       ortam değişkenleri ve doğrulama
  errors.py       kararlı hata kodları
  listener.py     hello bariyeri, replay, kuyruk, worker ve drain
  logging.py      stderr JSON logları ve sır maskeleme
  models.py       giriş/çıkış veri modelleri
  normalize.py    saf event/post normalizasyonu
  session.py      takılabilir SessionResolver
  state.py        takılabilir StateStore ve SQLite uygulaması
  transport.py    REST, websocket, watchdog ve reconnect
tests/acceptance/  dış sözleşme testleri
tests/ported/      kaynak karakterizasyon testlerinin portları
```

## CLI komutları

Bütün tek seferlik komutlar stdout'a tek satırlık şu zarfı yazar:

```json
{"status":"success","data":{},"error":null}
```

Hata zarfı `{"status":"error","data":null,"error":{"code":"...","message":"..."}}` biçimindedir. Loglar yalnız stderr'e gider.

### `normalize-event [--bot-user-id ID]`

stdin'den çıplak Mattermost Post nesnesi veya websocket olayı okur. Ağ bağlantısı ve yapılandırma gerekmez.

```sh
printf '%s\n' '{"id":"p2","channel_id":"c1","root_id":"p1","user_id":"u2","message":"merhaba","create_at":1789380000000,"type":"","props":{}}' \
  | mattermost-adapter normalize-event
```

```json
{"status":"success","data":{"action":"process","ignore_reason":null,"message":{"provider":"mattermost","channel_id":"c1","thread_id":"p1","message_id":"p2","parent_message_id":"p1","sender":{"id":"u2","display_name":null},"content":"merhaba","attachments":[],"timestamp":"2026-09-14T10:00:00.000Z","raw_metadata":{"post_type":"","create_at":1789380000000,"channel_type":null,"team_id":null,"props":{}}},"session":{"session_id":"mattermost:p1","attributes":{}}},"error":null}
```

### `send-message`

stdin alanları: zorunlu `channel_id` ve `text`; isteğe bağlı `thread_id`, `reply_to_message_id`, `props`.

```sh
printf '%s\n' '{"channel_id":"c1","thread_id":"p1","text":"Yanıt","reply_to_message_id":"p2","props":{"trace":"abc"}}' \
  | mattermost-adapter send-message
```

```json
{"status":"success","data":{"message_id":"new-post-id","channel_id":"c1","thread_id":"p1","create_at":1789380000123},"error":null}
```

### `fetch-file --file-id ID --output-dir DIR`

Dosya adından dizin parçaları atılır. Aynı ad varsa mevcut dosyanın üzerine yazmak yerine `name (1).ext` kullanılır.

```sh
mattermost-adapter fetch-file --file-id f1 --output-dir ./downloads
```

```json
{"status":"success","data":{"file_id":"f1","name":"rapor.pdf","size":12,"mime_type":"application/pdf","path":"/work/downloads/rapor.pdf"},"error":null}
```

### `health [--check-connection]`

Bayrak yokken token zorunlu değildir ve ağ çağrısı yapılmaz. Bayrak verildiğinde login sınanır.

```sh
mattermost-adapter health --check-connection
```

```json
{"status":"success","data":{"config":{"url":"localhost","port":8065,"scheme":"http","token_present":true},"connection":{"ok":true,"bot_user_id":"bot-id","username":"assistant-bot"}},"error":null}
```

### `listen`

Websocket'e bağlanır ve yalnız işlenecek mesajları stdout'a NDJSON olarak yazar. İlk cursor websocket açılmadan kurulur; sonraki bağlantılarda kanal başına en yeni 100 kaçırılmış post replay edilir. `SIGINT` ve `SIGTERM` yeni alımı durdurur, kabul edilmiş kuyruğu drain eder ve `0` ile çıkar.

```sh
MM_ADAPTER_CHANNELS=c1,c2 mattermost-adapter listen
```

```json
{"type":"message","correlation_id":"17dbb27b-57dd-44f8-9cca-eca05e822d4c","data":{"action":"process","ignore_reason":null,"message":{"provider":"mattermost","channel_id":"c1","thread_id":"p1","message_id":"p2","parent_message_id":"p1","sender":{"id":"u2","display_name":"@ayse"},"content":"merhaba","attachments":[],"timestamp":"2026-09-14T10:00:00.000Z","raw_metadata":{"post_type":"","create_at":1789380000000,"channel_type":"O","team_id":"t1","props":{}}},"session":{"session_id":"mattermost:p1","attributes":{}}}}
```

## Plugin kullanımı

Bir plugin, uzun yaşayan `listen` sürecinin stdout'unu satır satır okuyup her `message` kaydını kendi handler'ına verebilir. Yanıtı aynı thread'e `send-message` ile yollar:

```python
import json
import subprocess

listener = subprocess.Popen(
    ["mattermost-adapter", "listen"],
    stdout=subprocess.PIPE,
    text=True,
)

for line in listener.stdout:
    event = json.loads(line)
    if event.get("type") != "message":
        continue
    normalized = event["data"]["message"]
    answer = plugin_handle(event["data"]["session"], normalized)
    request = {
        "channel_id": normalized["channel_id"],
        "thread_id": normalized["thread_id"],
        "reply_to_message_id": normalized["message_id"],
        "text": answer,
    }
    subprocess.run(
        ["mattermost-adapter", "send-message"],
        input=json.dumps(request),
        text=True,
        check=True,
    )
```

Üretim plugin'i stderr'i de sürekli tüketmelidir; aksi halde işletim sistemi pipe tamponu dolup alt süreci durdurabilir.

## Çıkış kodları

- `0`: başarı
- `1`: `INTERNAL_ERROR`
- `2`: `INVALID_INPUT` veya `USAGE_ERROR`
- `3`: `CONFIG_ERROR`
- `4`: `AUTH_ERROR`
- `5`: `MATTERMOST_API_ERROR`

## Teknik borç

- Replay kanal başına 100 post ile sınırlıdır; taşan eski postlar uyarı loguyla atlanır ve ayrı bir arşiv kurtarma aracı yoktur.
- SQLite tek süreçli CLI modeli için tasarlanmıştır; çoklu listener süreçleri için sahiplik kilidi bulunmaz.
- Senkron `MessageHandler` ve durum/REST işleri `asyncio.to_thread` ile çalışır. Python thread'leri zorla iptal edilemediğinden deadline aşımı yalnız uyarı üretir; reconnect gerçek drain bitene kadar bekler.
- NDJSON akışı teslim onayı protokolü içermez. Handler satırı flush edildikten sonra cursor ilerler; tüketici satırı aldıktan hemen sonra çökerse uçtan uca exactly-once garanti edilmez.
- Websocket ve REST için üçüncü taraf sürücü korunmuştur; sürücü sürüm yükseltmeleri protokol karakterizasyon testleriyle yeniden doğrulanmalıdır.
