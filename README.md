# Bot Telegram per monitorare treni Trenitalia

Prototipo autonomo. Nessuna porta in ingresso.

## Configurazione
Obbligatorie in .env:
* TELEGRAM_BOT_TOKEN
* TELEGRAM_ADMIN_ID

Opzionali via ambiente (default):
* TIMEZONE (Europe/Rome)
* POLL_SECONDS (60)
* ERROR_ALERT_AFTER_MINUTES (10)
* CACHE_SECONDS (45)
* MAX_USERS (50)
* MAX_SUBSCRIPTIONS_PER_USER (10)
* NOTIFICATION_TTL_SECONDS (900)
* MAX_SEND_ATTEMPTS (12)
* ERROR_SUMMARY_SECONDS (300)
* VIAGGIATRENO_BASE (https://www.viaggiatreno.it/infomobilita/resteasy/viaggiatreno)
* REGISTRATION_MODE (**approval**|allowlist|open)
* DB_PATH (/data/state.sqlite)
* LOG_LEVEL (INFO)

I preferiti si gestiscono solo via bot (/aggiungi, /modifica, /elimina); weekdays: 0=lunedi, 6=domenica. Nel wizard i giorni accettano anche "oggi" o "domani" per monitorare un treno per un solo giorno (il preferito scade automaticamente a data passata). Le finestre non possono attraversare mezzanotte.
La stazione e risolta per nome esatto ignorando maiuscole e spazi ripetuti.
Non sono previsti menu interattivi per modificare i preferiti in questa versione.

## Comportamento
Controlla nelle sole finestre configurate. Frequenza nominale 60 secondi piu latenza I/O.
Prima notifica al raggiungimento della soglia; successive per variazioni assolute di almeno
5 minuti rispetto all'ultimo avviso; avviso quando il ritardo rientra sotto soglia.
Il valore notificato e il ritardo corrente dell'intero treno, NON una previsione certa per Ottavia.
Termina il monitoraggio giornaliero quando la partenza reale dalla fermata risulta valorizzata.
Se non risulta partito continua fino alla fine della finestra, anche se in forte ritardo.
Segnala dati indisponibili dopo 10 minuti; non interpreta l'assenza di dati come puntualita.
Non implementa riconoscimento soppressioni o cambi binario: servono verifiche su risposte reali.
/stato mostra solo i preferiti nella finestra attuale (giorni e fascia oraria configurati); non modifica la cronologia degli avvisi.
/pausa sospende fino a /riprendi, anche dopo riavvio.
La cronologia e persistente nel volume Docker. Conserva soltanto lo stato piu recente per treno.

## Limiti e verifica
Prototipo: controlli statici e test locali con dati simulati, NON collaudato con Docker,
Telegram e risposte reali ViaggiaTreno. Nessuna garanzia di disponibilita della fonte.
Endpoint non ufficiali e schema soggetto a cambiamenti. Nessuna evasione di blocchi HTTP:
se HTTPS o il servizio non rispondono, verificare la fonte prima di cambiare configurazione.
Prima di affidarti agli avvisi usa /stato durante la corsa e confronta con ViaggiaTreno/RFI.
Messaggi duplicati sono possibili se il processo si interrompe fra invio e commit SQLite;
nessuna garanzia exactly-once. Non viene controllata automaticamente la freschezza del
rilevamento: il messaggio riporta l'ultimo orario, quando presente.

## Sicurezza
.env e escluso dall'immagine tramite .dockerignore e da Git tramite .gitignore.
Il token e una credenziale: proteggi .env e revocalo con BotFather se esposto.
Container non privilegiato, nessuna dipendenza da QNAP, host networking o device.
Immagine basata su python:3.12-slim: la build necessita accesso al registry.
Serve un host per container Linux e un'architettura supportata dall'immagine base.

## Log
 docker compose logs -f --tail=100

## Fonti tecniche
https://core.telegram.org/bots/tutorial
https://github-wiki-see.page/m/roughconsensusandrunningcode/TrainMonitor/wiki/API-del-sistema-Viaggiatreno
