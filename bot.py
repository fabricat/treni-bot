import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from urllib.error import URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from telegram import BotCommand, BotCommandScopeChat, LinkPreviewOptions, Update
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError
from telegram.ext import ApplicationBuilder, MessageHandler, filters
from bot_logging import setup, event, unexpected, FailureTracker


class ApiError(Exception):
    def __init__(self, code, retry_after=None):
        super().__init__(f"Telegram API status {code}")
        self.code = code
        self.retry_after = retry_after


class SourceError(Exception):
    pass


class App:
    def __init__(self, config, db_path, token, admin_id):
        self.config = config
        self.db_path = str(db_path)
        self.token = token
        self.admin = admin_id
        self.tz = ZoneInfo(config.get("timezone", "Europe/Rome"))
        self.base = config.get("viaggiatreno_base", "https://www.viaggiatreno.it/infomobilita/resteasy/viaggiatreno").rstrip("/")
        self.local = threading.local()
        self.stop = threading.Event()
        self.failures = FailureTracker(config.get("error_summary_seconds", 300))
        self.cache = {}
        self.cache_lock = threading.Lock()
        self.poll_seconds = max(30, config.get("poll_seconds", 60))
        self.application = None
        self.loop = None
        self.workers = []
        self.watch_task = None
        self.worker_failed = False

    def db(self):
        if not hasattr(self.local, "db"):
            conn = sqlite3.connect(self.db_path, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=15000")
            self.local.db = conn
        return self.local.db

    def initialize(self):
        db = self.db()
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT, telegram_id INTEGER UNIQUE NOT NULL,
            chat_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            paused INTEGER NOT NULL DEFAULT 0, timezone TEXT NOT NULL,
            created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS subscriptions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name TEXT NOT NULL, number TEXT NOT NULL, station TEXT NOT NULL,
            weekdays TEXT NOT NULL, start TEXT NOT NULL, end TEXT NOT NULL,
            threshold INTEGER NOT NULL, change_min INTEGER NOT NULL DEFAULT 5,
            enabled INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS conversations(
            user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            step TEXT NOT NULL, data TEXT NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS notification_state(
            subscription_id INTEGER PRIMARY KEY REFERENCES subscriptions(id) ON DELETE CASCADE,
            date TEXT NOT NULL, last_delay REAL, done INTEGER NOT NULL DEFAULT 0,
            error_since REAL, error_notified INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS outbox(
            id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            subscription_id INTEGER REFERENCES subscriptions(id) ON DELETE CASCADE,
            chat_id INTEGER NOT NULL, text TEXT NOT NULL, dedup TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL, created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS outbox_ready ON outbox(status,next_attempt);
        ''')
        with db:
            db.execute("INSERT INTO users(telegram_id,chat_id,status,timezone,created) VALUES (?,?,'active',?,?) ON CONFLICT(telegram_id) DO UPDATE SET status='active',chat_id=excluded.chat_id", (self.admin,self.admin,str(self.tz),time.time()))
            if not db.execute("SELECT 1 FROM meta WHERE key='schema_version'").fetchone():
                owner = db.execute("SELECT id FROM users WHERE telegram_id=?",(self.admin,)).fetchone()[0]
                for t in self.config.get("trains",[]):
                    db.execute("INSERT INTO subscriptions(user_id,name,number,station,weekdays,start,end,threshold,change_min) VALUES (?,?,?,?,?,?,?,?,?)",(owner,t.get("name",t["number"]),str(t["number"]),t["boarding_station"],json.dumps(t["weekdays"]),t["from"],t["until"],t.get("threshold_minutes",5),t.get("change_minutes",5)))
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='state'").fetchone():
                    old = db.execute("SELECT value FROM state WHERE key='paused'").fetchone()
                    if old and json.loads(old[0]):
                        db.execute("UPDATE users SET paused=1 WHERE id=?",(owner,))
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='state'").fetchone():
                    old_offset=db.execute("SELECT value FROM state WHERE key='offset'").fetchone()
                    if old_offset:
                        db.execute("INSERT OR REPLACE INTO meta VALUES ('telegram_offset',?)",(str(json.loads(old_offset[0])),))
                db.execute("INSERT INTO meta VALUES ('schema_version','2')")
                event(logging.INFO,"database.migrated",schema=2)

    def request(self, url, timeout=20):
        operation = urlsplit(url).path.removeprefix("/infomobilita/resteasy/viaggiatreno/").split('/')[0]
        req_id = uuid.uuid4().hex[:10]
        began = time.monotonic()
        event(logging.DEBUG,"http.request",request=req_id,service="viaggiatreno",operation=operation)
        req = Request(url,headers={"User-Agent":"PersonalTrainNotifier/2.0"})
        with urlopen(req,timeout=timeout) as response:
            body = response.read()
            status = response.status
        event(logging.DEBUG,"http.completed",request=req_id,service="viaggiatreno",operation=operation,status=status,duration_ms=round((time.monotonic()-began)*1000),bytes=len(body))
        return body.decode('utf-8')

    def send_message(self, chat_id, text):
        future = asyncio.run_coroutine_threadsafe(self.application.bot.send_message(chat_id=chat_id,text=text,parse_mode='HTML',link_preview_options=LinkPreviewOptions(is_disabled=True)),self.loop)
        try:
            future.result(timeout=60)
        except Forbidden:
            raise ApiError(403) from None
        except RetryAfter as exc:
            raise ApiError(429,int(exc.retry_after)) from None
        except BadRequest:
            raise ApiError(400) from None
        except NetworkError:
            raise ApiError(500) from None
        except TelegramError:
            raise ApiError(400) from None
        except TimeoutError:
            future.cancel()
            raise ApiError(504) from None

    def queue(self, chat, text, user=None, sub=None, dedup=None, conn=None):
        db = conn or self.db()
        db.execute("INSERT OR IGNORE INTO outbox(user_id,subscription_id,chat_id,text,dedup,next_attempt,created) VALUES (?,?,?,?,?,?,?)",(user,sub,chat,text,dedup or uuid.uuid4().hex,time.time(),time.time()))
        if conn is None:
            db.commit()

    def reply(self, user, text, key=None):
        self.queue(user['chat_id'],text,user=user['id'],dedup=key)

    def conversation(self,user,step,data):
        with self.db() as db:
            db.execute("INSERT OR REPLACE INTO conversations VALUES (?,?,?,?)",(user['id'],step,json.dumps(data),time.time()))

    def user(self, telegram_id):
        return self.db().execute("SELECT * FROM users WHERE telegram_id=?",(telegram_id,)).fetchone()

    def subscriptions(self,user_id):
        return self.db().execute("SELECT * FROM subscriptions WHERE user_id=? ORDER BY id",(user_id,)).fetchall()

    def train_link(self,number,run_key,label='Vedi su ViaggiaTreno'):
        origin,_,millis=run_key.split(':')
        url=f"http://www.viaggiatreno.it/infomobilitamobile/pages/cercaTreno/cercaTreno.jsp?treno={quote(str(number))}&origine={quote(origin)}&datapartenza={quote(millis)}"
        return f'<a href="{html.escape(url,quote=True)}">{html.escape(str(label))}</a>'

    def known_link(self,sub,conn=None,label='Vedi su ViaggiaTreno'):
        # run_key dell'ultima corsa valutata oggi, salvato in notification_state.date come 'data|origine:numero:millis'
        row=(conn or self.db()).execute('SELECT date FROM notification_state WHERE subscription_id=?',(sub['id'],)).fetchone()
        if row and row['date'].startswith(datetime.now(self.tz).date().isoformat()) and '|' in row['date']:
            return '\n'+self.train_link(sub['number'],row['date'].split('|',1)[1],label)
        return ''

    def list_preferences(self,user):
        days=['lun','mar','mer','gio','ven','sab','dom']
        subs=self.subscriptions(user['id'])
        head=f"Pausa: {'si' if user['paused'] else 'no'}"
        if not subs:
            return head+'\nNessun preferito: /aggiungi'
        lines=[head]
        for s in subs:
            wd=json.loads(s['weekdays'])
            link=self.known_link(s,label=s['number'])
            number=link[1:] if link else s['number']
            lines.append(f"• Treno {number} da {html.escape(s['station'])} | {'tutti' if len(wd)==7 else ','.join(days[d] for d in wd)} | {s['start']}-{s['end']} | soglia {s['threshold']} min")
        return '\n'.join(lines)

    def handle(self, update):
        msg=update.get('message',{})
        chat=msg.get('chat',{})
        if chat.get('type')!='private' or not isinstance(msg.get('text'),str):
            return
        sender=msg.get('from',{}).get('id')
        text=msg['text'].strip()
        parts=text.split()
        if not parts:
            return
        cmd=parts[0].split('@')[0].lower() if text.startswith('/') else ''
        user=self.user(sender)
        known={'/start','/id','/aiuto','/preferiti','/aggiungi','/modifica','/elimina','/stato','/pausa','/riprendi','/annulla','/conferma','/approva','/rifiuta','/utenti','/cancella_account'}
        event(logging.INFO if user and user['status']=='active' else logging.DEBUG,'command.received',user=user['id'] if user else None,command=cmd if cmd in known else 'input')
        if cmd=='/id':
            self.queue(chat['id'],f"Il tuo Telegram user ID: {sender}"+(f"\nID interno: {user['id']}" if user else ''))
            return
        if cmd=='/start' and user and user['status']=='unreachable':
            with self.db() as db:db.execute("UPDATE users SET status='active' WHERE id=?",(user['id'],))
            user=self.user(sender)
        if cmd=='/start' and not user:
            mode=os.environ.get('REGISTRATION_MODE','approval')
            allowed={int(v) for v in os.environ.get('ALLOWED_TELEGRAM_IDS','').split(',') if v.strip().isdigit()}
            if mode=='allowlist' and sender not in allowed:
                self.queue(chat['id'],'Accesso non autorizzato.')
                return
            if self.db().execute("SELECT count(*) FROM users").fetchone()[0]>=self.config.get('max_users',50):
                self.queue(chat['id'],'Registrazioni temporaneamente chiuse.')
                return
            status='active' if mode in ('open','allowlist') else 'pending'
            with self.db() as db:
                db.execute("INSERT INTO users(telegram_id,chat_id,status,timezone,created) VALUES (?,?,?,?,?)",(sender,chat['id'],status,str(self.tz),time.time()))
            user=self.user(sender)
            event(logging.INFO,'user.registered',user=user['id'],status=status)
            if status=='pending':
                admin=self.user(self.admin)
                self.reply(admin,f"Richiesta accesso utente interno #{user['id']}.\n/approva {user['id']}\n/rifiuta {user['id']}",key=f"approval:{user['id']}")
        if not user:
            self.queue(chat['id'],'Invia /start per registrarti.')
            return
        if user['status']!='active':
            self.reply(user,'Accesso in attesa di approvazione o sospeso.')
            return
        if cmd=='/start':
            with self.db() as db:
                db.execute('UPDATE users SET chat_id=? WHERE id=?',(chat['id'],user['id']))
        admin=sender==self.admin
        if cmd in ('/approva','/rifiuta','/utenti'):
            if not admin:
                self.reply(user,'Comando riservato all amministratore.')
                return
            if cmd=='/utenti':
                rows=self.db().execute('SELECT id,status,paused FROM users ORDER BY id').fetchall()
                self.reply(user,'\n'.join(f"#{r['id']} {r['status']} pausa={r['paused']}" for r in rows))
                return
            if len(parts)!=2 or not parts[1].isdigit():
                self.reply(user,f"Uso: {cmd} ID_INTERNO")
                return
            target=self.db().execute('SELECT * FROM users WHERE id=?',(int(parts[1]),)).fetchone()
            if not target or target['telegram_id']==self.admin:
                self.reply(user,'Utente non valido.')
                return
            status='active' if cmd=='/approva' else 'denied'
            with self.db() as db:
                db.execute('UPDATE users SET status=? WHERE id=?',(status,target['id']))
                if status!='active':
                    db.execute("UPDATE outbox SET status='cancelled' WHERE user_id=? AND status='pending'",(target['id'],))
            self.reply(target,'Accesso approvato. Usa /aggiungi.' if status=='active' else 'Accesso non autorizzato.')
            self.reply(user,f"Utente #{target['id']}: {status}")
            event(logging.INFO,'user.access_changed',user=target['id'],status=status)
            return
        if cmd in ('/start','/aiuto'):
            self.reply(user,'Bot treni personale\n/preferiti\n/aggiungi\n/modifica NUMERO_TRENO\n/elimina NUMERO_TRENO\n/stato\n/pausa\n/riprendi\n/annulla\n/cancella_account\nI giorni e gli orari sono personali.\n'+('/utenti - /approva ID - /rifiuta ID' if admin else ''))
        elif cmd=='/annulla':
            with self.db() as db: db.execute('DELETE FROM conversations WHERE user_id=?',(user['id'],))
            self.reply(user,'Operazione annullata.')
        elif cmd=='/preferiti': self.reply(user,self.list_preferences(user))
        elif cmd in ('/pausa','/riprendi'):
            with self.db() as db:
                db.execute('UPDATE users SET paused=? WHERE id=?',(int(cmd=='/pausa'),user['id']))
                if cmd=='/pausa':
                    db.execute("UPDATE outbox SET status='cancelled' WHERE user_id=? AND subscription_id IS NOT NULL AND status='pending'",(user['id'],))
            self.reply(user,'Avvisi sospesi.' if cmd=='/pausa' else 'Avvisi riattivati nelle finestre configurate.')
            event(logging.INFO,'user.pause_changed',user=user['id'],paused=cmd=='/pausa')
        elif cmd in ('/aggiungi','/modifica'):
            data={}
            if cmd=='/modifica':
                sub=self.owned_sub(user,parts)
                if not sub:return
                data['edit_id']=sub['id']
            elif len(self.subscriptions(user['id']))>=self.config.get('max_subscriptions_per_user',10):
                self.reply(user,'Limite preferiti raggiunto.');return
            self.conversation(user,'number',data)
            self.reply(user,'Numero del treno? (/annulla per uscire)')
        elif cmd=='/elimina':
            sub=self.owned_sub(user,parts)
            if not sub:return
            self.conversation(user,'delete',{'id':sub['id']})
            self.reply(user,f"Eliminare il treno {sub['number']} da {html.escape(sub['station'])}? /conferma o /annulla")
        elif cmd=='/cancella_account':
            if admin:
                self.reply(user,'Account amministratore non eliminabile da questo comando.');return
            self.conversation(user,'delete_account',{})
            self.reply(user,'Eliminare account e preferenze? /conferma o /annulla')
        elif cmd=='/stato':
            with self.db() as db:
                db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',(f"manual:{user['id']}",str(time.time())))
            self.reply(user,'Richiesta di stato accodata.')
        else:
            self.wizard(user,text,cmd)

    def owned_sub(self,user,parts):
        if len(parts)<2 or not parts[1].isdigit():
            self.reply(user,'Indica il numero del treno, visibile in /preferiti.');return None
        norm=lambda v:' '.join(str(v).upper().split())
        subs=self.db().execute('SELECT * FROM subscriptions WHERE user_id=? AND number=? ORDER BY id',(user['id'],parts[1])).fetchall()
        if len(parts)>2:
            subs=[s for s in subs if norm(s['station'])==norm(' '.join(parts[2:]))]
        if not subs:
            self.reply(user,'Preferito non trovato.');return None
        if len(subs)>1:
            cmd=parts[0].split('@')[0].lower()
            self.reply(user,'Piu preferiti con questo numero. Specifica la stazione:\n'+'\n'.join(f"{cmd} {s['number']} {html.escape(s['station'])}" for s in subs));return None
        return subs[0]

    def wizard(self,user,text,cmd):
        row=self.db().execute('SELECT * FROM conversations WHERE user_id=?',(user['id'],)).fetchone()
        if not row:
            self.reply(user,'Comando non riconosciuto. /aiuto');return
        if time.time()-row['updated']>1800:
            with self.db() as db:db.execute('DELETE FROM conversations WHERE user_id=?',(user['id'],))
            self.reply(user,'Procedura scaduta. Riparti con /aggiungi o /modifica NUMERO_TRENO.');return
        data=json.loads(row['data']);step=row['step']
        try:
            if step=='number':
                if not re.fullmatch(r'\d{1,7}',text):raise ValueError('Inserisci un numero treno valido.')
                data['number']=text;step='station';prompt='Stazione di salita? Scrivi il nome completo, ad esempio Ottavia.'
            elif step=='station':
                if cmd or not 2<=len(text)<=100:raise ValueError('Nome stazione non valido.')
                data['station']=text;step='days';prompt='Giorni? Scrivi lun,mar,mer,gio,ven oppure tutti.'
            elif step=='days':
                names={'lun':0,'mar':1,'mer':2,'gio':3,'ven':4,'sab':5,'dom':6}
                values=re.split(r'[,\s]+',text.lower())
                if text.lower()=='tutti':data['days']=list(range(7))
                else:
                    if not values or any(v not in names for v in values):raise ValueError('Usa lun,mar,mer,gio,ven,sab,dom.')
                    data['days']=sorted({names[v] for v in values})
                step='window';prompt=f"Finestra locale? (es. 06:50-09:00) Non puo attraversare mezzanotte."
            elif step=='window':
                if not re.fullmatch(r'\d{2}:\d{2}-\d{2}:\d{2}',text):raise ValueError('Formato richiesto HH:MM-HH:MM.')
                start,end=text.split('-')
                datetime.strptime(start,'%H:%M');datetime.strptime(end,'%H:%M')
                if start>=end:raise ValueError('Inizio deve precedere fine nello stesso giorno.')
                data.update(start=start,end=end);step='threshold';prompt='Soglia ritardo in minuti? Da 1 a 180.'
            elif step=='threshold':
                if not text.isdigit() or not 1<=int(text)<=180:raise ValueError('Soglia valida: 1-180.')
                data['threshold']=int(text);step='change';prompt='Variazione minima per un nuovo avviso? Da 1 a 60 minuti (consigliato 5).'
            elif step=='change':
                if not text.isdigit() or not 1<=int(text)<=60:raise ValueError('Valore valido: 1-60.')
                data['change']=int(text);step='confirm'
                prompt=(f"Treno {data['number']} da {html.escape(data['station'])}\nGiorni: {','.join(['lun','mar','mer','gio','ven','sab','dom'][d] for d in data['days'])}\n"
                        f"Fascia {data['start']}-{data['end']} ({self.tz})\nSoglia {data['threshold']} min, variazione {data['change']} min.\n"
                        'Verifica della corsa rinviata alla prima interrogazione: non validata online in questa procedura.\n/conferma o /annulla')
            elif step in ('confirm','delete','delete_account'):
                if cmd!='/conferma':raise ValueError('Usa /conferma oppure /annulla.')
                with self.db() as db:
                    if step=='delete_account':
                        db.execute('DELETE FROM users WHERE id=?',(user['id'],))
                        db.execute('DELETE FROM meta WHERE key=?',(f"manual:{user['id']}",))
                    elif step=='delete':
                        db.execute('DELETE FROM subscriptions WHERE id=? AND user_id=?',(data['id'],user['id']))
                    else:
                        duplicate=db.execute('SELECT id FROM subscriptions WHERE user_id=? AND number=? AND upper(station)=upper(?) AND id!=?',(user['id'],data['number'],data['station'],data.get('edit_id',-1))).fetchone()
                        if duplicate:raise ValueError('Hai gia questo treno e stazione. Modifica il preferito esistente.')
                        args=(data['number'],data['number'],data['station'],json.dumps(data['days']),data['start'],data['end'],data['threshold'],data['change'])
                        if data.get('edit_id'):
                            cur=db.execute('UPDATE subscriptions SET name=?,number=?,station=?,weekdays=?,start=?,end=?,threshold=?,change_min=? WHERE id=? AND user_id=?',args+(data['edit_id'],user['id']))
                            if not cur.rowcount:raise ValueError('Preferito non piu disponibile.')
                            db.execute('DELETE FROM notification_state WHERE subscription_id=?',(data['edit_id'],))
                            db.execute("UPDATE outbox SET status='cancelled' WHERE subscription_id=? AND status='pending'",(data['edit_id'],))
                        else:
                            if db.execute('SELECT count(*) FROM subscriptions WHERE user_id=?',(user['id'],)).fetchone()[0]>=self.config.get('max_subscriptions_per_user',10):raise ValueError('Limite preferiti raggiunto.')
                            db.execute('INSERT INTO subscriptions(name,number,station,weekdays,start,end,threshold,change_min,user_id) VALUES (?,?,?,?,?,?,?,?,?)',args+(user['id'],))
                    db.execute('DELETE FROM conversations WHERE user_id=?',(user['id'],))
                if step=='delete_account':self.queue(user['chat_id'],'Account e preferenze eliminati.')
                else:self.reply(user,'Operazione completata.')
                event(logging.INFO,'preferences.changed',user=user['id'],action=step)
                return
            else:raise ValueError('Stato procedura non valido. /annulla')
            self.conversation(user,step,data);self.reply(user,prompt)
        except ValueError as exc:self.reply(user,str(exc))

    def fetch_train(self,number,now):
        key=(number,now.date().isoformat())
        with self.cache_lock:
            cached=self.cache.get(key)
            if cached and time.monotonic()-cached[0]<self.config.get('cache_seconds',45):
                event(logging.DEBUG,'train.cache_hit',train=number);return cached[1]
        raw=self.request(self.base+'/cercaNumeroTrenoTrenoAutocomplete/'+quote(number))
        candidates=[]
        for n,origin,millis in set(re.findall(r'\|\s*(\d+)-(S\d+)-(\d+)',raw)):
            if n!=number or datetime.fromtimestamp(int(millis)/1000,self.tz).date()!=now.date():continue
            data=json.loads(self.request(self.base+f'/andamentoTreno/{origin}/{n}/{millis}'))
            if isinstance(data,dict):
                data['_run_key']=f'{origin}:{n}:{millis}';candidates.append(data)
        if not candidates:raise SourceError('Corsa non disponibile per la data richiesta')
        with self.cache_lock:
            self.cache={k:v for k,v in self.cache.items() if time.monotonic()-v[0]<120}
            self.cache[key]=(time.monotonic(),candidates)
        return candidates

    def choose(self,candidates,station):
        norm=lambda v:' '.join(str(v).upper().split())
        found=[]
        for d in candidates:
            stops=[s for s in d.get('fermate',[]) if norm(s.get('stazione'))==norm(station)]
            if len(stops)==1:found.append((d,stops[0]))
        if len(found)!=1:raise SourceError('Corsa/fermata assente o ambigua')
        return found[0]

    def describe(self,sub,data,stop):
        delay=data.get('ritardo')
        if isinstance(delay,bool) or not isinstance(delay,(int,float)):raise SourceError('Ritardo non numerico')
        fmt=lambda ms:datetime.fromtimestamp(float(ms)/1000,self.tz).strftime('%H:%M') if ms else 'n/d'
        text=f"Treno {sub['number']} - {html.escape(sub['station'])}\nPartenza prevista: {fmt(stop.get('partenza_teorica'))}\nRitardo corrente del treno: {delay:+g} min"
        if stop.get('partenzaReale'):text+='\nPartito dalla fermata: '+fmt(stop['partenzaReale'])
        if data.get('oraUltimoRilevamento'):text+='\nUltimo rilevamento: '+fmt(data['oraUltimoRilevamento'])
        return delay,text+'\nIl ritardo corrente non e una previsione garantita alla tua fermata.\n'+self.train_link(sub['number'],data['_run_key'])

    def evaluate(self,sub,user,data,stop,now):
        delay,text=self.describe(sub,data,stop)
        date=now.date().isoformat()+'|'+data['_run_key']
        with self.db() as db:
            current=db.execute('SELECT * FROM subscriptions WHERE id=? AND user_id=?',(sub['id'],user['id'])).fetchone()
            fresh=db.execute('SELECT * FROM users WHERE id=?',(user['id'],)).fetchone()
            if not current or dict(current)!=dict(sub) or not fresh or fresh['status']!='active' or fresh['paused']:return
            state=db.execute('SELECT * FROM notification_state WHERE subscription_id=?',(sub['id'],)).fetchone()
            if not state or state['date']!=date:
                db.execute('INSERT OR REPLACE INTO notification_state(subscription_id,date) VALUES (?,?)',(sub['id'],date))
                state=db.execute('SELECT * FROM notification_state WHERE subscription_id=?',(sub['id'],)).fetchone()
            if state['done']:return
            last=state['last_delay'];reason=None
            if delay>=sub['threshold'] and (last is None or abs(delay-last)>=sub['change_min']):reason='threshold_reached' if last is None else 'delay_changed'
            elif delay<sub['threshold'] and last is not None and last>=sub['threshold']:reason='below_threshold'
            if state['error_notified']:
                self.queue(user['chat_id'],f"Treno {sub['number']}: fonte dati nuovamente disponibile.\n{self.train_link(sub['number'],data['_run_key'])}",user=user['id'],sub=sub['id'],conn=db)
            db.execute('UPDATE notification_state SET error_since=NULL,error_notified=0 WHERE subscription_id=?',(sub['id'],))
            if reason:
                self.queue(user['chat_id'],f"Avviso: {reason}\n{text}",user=user['id'],sub=sub['id'],conn=db)
                db.execute('UPDATE notification_state SET last_delay=? WHERE subscription_id=?',(delay,sub['id']))
                event(logging.INFO,'notification.queued',user=user['id'],subscription=sub['id'],train=sub['number'],delay_min=delay,reason=reason)
            if stop.get('partenzaReale'):db.execute('UPDATE notification_state SET done=1 WHERE subscription_id=?',(sub['id'],))
        event(logging.DEBUG,'subscription.checked',user=user['id'],subscription=sub['id'],train=sub['number'],delay_min=delay,notify=bool(reason))

    def source_unavailable(self,sub,user,date):
        now=time.time()
        with self.db() as db:
            if not db.execute('SELECT 1 FROM subscriptions WHERE id=?',(sub['id'],)).fetchone():return
            state=db.execute('SELECT * FROM notification_state WHERE subscription_id=?',(sub['id'],)).fetchone()
            if not state or not state['date'].startswith(date):
                db.execute('INSERT OR REPLACE INTO notification_state(subscription_id,date,error_since) VALUES (?,?,?)',(sub['id'],date,now))
                return
            if state['done']:return
            since=state['error_since'] or now
            db.execute('UPDATE notification_state SET error_since=? WHERE subscription_id=?',(since,sub['id']))
            if now-since>=self.config.get('error_alert_after_minutes',10)*60 and not state['error_notified']:
                link='\n'+self.train_link(sub['number'],state['date'].split('|',1)[1]) if '|' in state['date'] else ''
                self.queue(user['chat_id'],f"Treno {sub['number']}: dati indisponibili da almeno {self.config.get('error_alert_after_minutes',10)} minuti. Non significa puntualita o soppressione.{link}",user=user['id'],sub=sub['id'],conn=db)
                db.execute('UPDATE notification_state SET error_notified=1 WHERE subscription_id=?',(sub['id'],))

    def scan(self):
        db=self.db();now=datetime.now(self.tz)
        users=db.execute("SELECT * FROM users WHERE status='active'").fetchall()
        manual={int(r['key'].split(':')[1]):r['value'] for r in db.execute("SELECT key,value FROM meta WHERE key LIKE 'manual:%'")}
        groups={}
        for user in users:
            local=datetime.now(self.tz)
            is_manual=user['id'] in manual
            subs=self.subscriptions(user['id'])
            scheduled_count=0
            for sub in subs:
                # finestra personale: giorno configurato e orario start<=ora<end
                scheduled=local.weekday() in json.loads(sub['weekdays']) and sub['start']<=local.strftime('%H:%M')<sub['end']
                scheduled_count+=scheduled
                active=not user['paused'] and sub['enabled'] and scheduled
                if active or (is_manual and scheduled):groups.setdefault(sub['number'],[]).append((user,sub,active,is_manual and scheduled))
            if is_manual and not scheduled_count:self.reply(user,'Nessun preferito. /aggiungi' if not subs else 'Nessun preferito nella finestra attuale (giorni e fascia oraria). Vedi /preferiti.')
        for number,items in groups.items():
            try:
                candidates=self.fetch_train(number,now)
                self.failures.recovered('train:'+number)
            except Exception as exc:
                if not isinstance(exc,(URLError,OSError,ValueError,SourceError)):
                    unexpected('train.unexpected_failure',exc,train=number)
                self.failures.failed('train:'+number,exc)
                for user,sub,active,is_manual in items:
                    if active:self.source_unavailable(sub,user,now.date().isoformat())
                    if is_manual:self.reply(user,f"Treno {number}: dati non disponibili."+self.known_link(sub))
                continue
            checked=0
            for user,sub,active,is_manual in items:
                try:
                    data,stop=self.choose(candidates,sub['station'])
                    self.failures.recovered(f"subscription:{sub['id']}")
                    if active:self.evaluate(sub,user,data,stop,now)
                    if is_manual:self.reply(user,self.describe(sub,data,stop)[1]+'\nDati acquisiti di recente (cache massima '+str(self.config.get('cache_seconds',45))+' s).')
                    checked+=1
                except SourceError as exc:
                    self.failures.failed(f"subscription:{sub['id']}",exc)
                    if active:self.source_unavailable(sub,user,now.date().isoformat())
                    if is_manual:self.reply(user,f"Treno {number}: fermata/corsa non identificabile."+self.known_link(sub))
            event(logging.INFO,'train.checked',train=number,subscriptions=checked)
        with db:
            for uid,value in manual.items():db.execute('DELETE FROM meta WHERE key=? AND value=?',(f'manual:{uid}',value))
            db.execute("DELETE FROM outbox WHERE status!='pending' AND created<?",(time.time()-86400,))
            db.execute('DELETE FROM conversations WHERE updated<?',(time.time()-1800,))

    async def on_update(self,update,context):
        await asyncio.to_thread(self.handle,update.to_dict())

    async def on_error(self,update,context):
        unexpected('command.failed',context.error,update=update.update_id if isinstance(update,Update) else None)

    def monitoring(self):
        next_run=0
        while not self.stop.is_set():
            try:
                pending=self.db().execute("SELECT 1 FROM meta WHERE key LIKE 'manual:%' LIMIT 1").fetchone()
                if pending or time.monotonic()>=next_run:
                    self.scan();next_run=time.monotonic()+self.poll_seconds
            except Exception as exc:unexpected('monitoring.failed',exc);next_run=time.monotonic()+15
            self.stop.wait(2)

    def deliver_one(self):
        db=self.db()
        row=db.execute("SELECT * FROM outbox WHERE status='pending' AND next_attempt<=? ORDER BY id LIMIT 1",(time.time(),)).fetchone()
        if not row:return False
        if row['subscription_id']:
            user=db.execute('SELECT * FROM users WHERE id=?',(row['user_id'],)).fetchone()
            if not user or user['paused'] or user['status']!='active':
                with db:db.execute("UPDATE outbox SET status='cancelled' WHERE id=?",(row['id'],))
                return True
            if time.time()-row['created']>self.config.get('notification_ttl_seconds',900):
                with db:db.execute("UPDATE outbox SET status='failed' WHERE id=?",(row['id'],))
                event(logging.WARNING,'notification.expired',user=row['user_id'],outbox=row['id'])
                return True
        try:
            self.send_message(row['chat_id'],row['text'])
            with db:db.execute("UPDATE outbox SET status='sent' WHERE id=?",(row['id'],))
            self.failures.recovered('telegram.sender')
            event(logging.INFO,'notification.sent' if row['subscription_id'] else 'message.sent',user=row['user_id'],subscription=row['subscription_id'],outbox=row['id'])
        except ApiError as exc:
            attempts=row['attempts']+1
            if exc.code==403:
                with db:
                    db.execute("UPDATE outbox SET status='failed' WHERE id=?",(row['id'],))
                    if row['user_id']:
                        db.execute("UPDATE users SET status='unreachable' WHERE id=? AND status='active'",(row['user_id'],))
                        db.execute("UPDATE outbox SET status='cancelled' WHERE user_id=? AND status='pending'",(row['user_id'],))
                event(logging.WARNING,'user.unreachable',user=row['user_id'])
            elif exc.code==429 or exc.code>=500:
                delay=max(1,int(exc.retry_after or min(300,2**min(attempts,8))))
                self.retry(row,attempts,delay,exc)
                if exc.code==429:self.stop.wait(delay)
            else:
                with db:db.execute("UPDATE outbox SET status='failed' WHERE id=?",(row['id'],))
                event(logging.ERROR,'message.rejected',user=row['user_id'],outbox=row['id'],status=exc.code)
        except (URLError,OSError,ValueError) as exc:
            attempts=row['attempts']+1
            self.retry(row,attempts,min(300,2**min(attempts,8)),exc)
        return True

    def retry(self,row,attempts,delay,exc):
        expired=bool(row['subscription_id']) and time.time()-row['created']>self.config.get('notification_ttl_seconds',900)
        failed=expired or attempts>=self.config.get('max_send_attempts',12)
        with self.db() as db:db.execute('UPDATE outbox SET status=?,attempts=?,next_attempt=? WHERE id=?',('failed' if failed else 'pending',attempts,time.time()+delay,row['id']))
        self.failures.failed('telegram.sender',exc,retry_s=delay)
        if failed:event(logging.ERROR,'message.abandoned',user=row['user_id'],outbox=row['id'],attempts=attempts,expired=expired)

    def sending(self):
        while not self.stop.is_set():
            try:busy=self.deliver_one()
            except Exception as exc:unexpected('sender.failed',exc);busy=False
            self.stop.wait(1.1 if busy else 0.5)

    async def setup_commands(self,bot):
        commands=[
            BotCommand('start','Registrati o riattiva il bot'),
            BotCommand('aiuto','Guida ai comandi'),
            BotCommand('preferiti','Elenca i treni preferiti'),
            BotCommand('aggiungi','Aggiungi un treno preferito'),
            BotCommand('modifica','Modifica un preferito: /modifica NUMERO_TRENO'),
            BotCommand('elimina','Elimina un preferito: /elimina NUMERO_TRENO'),
            BotCommand('stato','Stato dei preferiti nella finestra attuale'),
            BotCommand('pausa','Sospendi gli avvisi'),
            BotCommand('riprendi','Riattiva gli avvisi'),
            BotCommand('annulla','Annulla l\'operazione in corso'),
            BotCommand('id','Mostra il tuo Telegram ID'),
            BotCommand('cancella_account','Elimina account e dati'),
        ]
        admin_commands=commands+[
            BotCommand('utenti','Elenca gli utenti'),
            BotCommand('approva','Approva un utente: /approva ID'),
            BotCommand('rifiuta','Rifiuta un utente: /rifiuta ID'),
        ]
        await bot.set_my_commands(commands)
        await bot.set_my_commands(admin_commands,scope=BotCommandScopeChat(chat_id=self.admin))

    async def watch(self,application):
        while not self.stop.is_set():
            if any(not worker.is_alive() for worker in self.workers):
                self.worker_failed=True
                application.stop_running()
                return
            await asyncio.sleep(1)

    async def post_init(self,application):
        self.loop=asyncio.get_running_loop()
        try:
            await self.setup_commands(application.bot);event(logging.INFO,'commands.registered')
        except Exception as exc:
            unexpected('commands.register_failed',exc)
        self.workers=[threading.Thread(target=target,name=name,daemon=True) for name,target in [('monitoring',self.monitoring),('sender',self.sending)]]
        for worker in self.workers:worker.start()
        self.watch_task=self.loop.create_task(self.watch(application))
        event(logging.INFO,'bot.started',users=self.db().execute('SELECT count(*) FROM users').fetchone()[0],subscriptions=self.db().execute('SELECT count(*) FROM subscriptions').fetchone()[0],registration=os.environ.get('REGISTRATION_MODE','approval'))

    async def post_stop(self,application):
        self.stop.set()
        if self.watch_task:
            self.watch_task.cancel()
            await asyncio.gather(self.watch_task,return_exceptions=True)
        for worker in self.workers:worker.join(timeout=45)
        event(logging.INFO,'bot.stopped')

    def run(self):
        self.initialize()
        self.application=ApplicationBuilder().token(self.token).post_init(self.post_init).post_stop(self.post_stop).build()
        self.application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT,self.on_update))
        self.application.add_error_handler(self.on_error)
        self.application.run_polling(timeout=20,bootstrap_retries=-1,allowed_updates=['message'],drop_pending_updates=False)
        if self.worker_failed:raise RuntimeError('Worker terminato inatteso')


def main():
    numeric=('error_summary_seconds','poll_seconds','cache_seconds','max_users','max_subscriptions_per_user','notification_ttl_seconds','error_alert_after_minutes','max_send_attempts')
    config={}
    for key in ('timezone','viaggiatreno_base')+numeric:
        value=os.environ.get(key.upper())
        if value:config[key]=int(value) if key in numeric else value
    setup(config.get('timezone','Europe/Rome'))
    token=os.environ.get('TELEGRAM_BOT_TOKEN','')
    admin=os.environ.get('TELEGRAM_ADMIN_ID')
    if not token or token.startswith('INSERISCI') or not admin.isdigit() or int(admin)<=0:raise ValueError('Imposta TELEGRAM_BOT_TOKEN e TELEGRAM_ADMIN_ID numerico positivo in .env')
    if os.environ.get('REGISTRATION_MODE','approval') not in ('approval','allowlist','open'):raise ValueError('REGISTRATION_MODE non valida')
    app=App(config,os.environ.get('DB_PATH','/data/state.sqlite'),token,int(admin))
    app.run()


if __name__=='__main__':
    try:main()
    except Exception as exc:
        if not logging.getLogger().handlers:setup('UTC')
        unexpected('bot.fatal',exc)
        raise SystemExit(1)
