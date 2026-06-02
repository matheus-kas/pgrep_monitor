# Configuração do Bot do Telegram

Guia para criar (ou reaproveitar) um bot do Telegram e fazer o
`pgrep_monitor` enviar os status para o seu Telegram.

> Resumo do fluxo: você cria um **bot** → o bot te dá um **token** →
> você descobre o seu **chat_id** → coloca os dois no arquivo `.env`.

---

## 1. Criar um bot novo com o @BotFather

O **BotFather** é o bot oficial do Telegram para criar outros bots.

1. No Telegram, procure por **@BotFather** (tem o selo azul de verificado) e abra a conversa.
2. Envie o comando:
   ```
   /newbot
   ```
3. Ele vai pedir um **nome** (livre, ex.: `Monitor Replica PostgreSQL`).
4. Depois pede um **username**, que precisa terminar em `bot` (ex.: `redeauto_pgmonitor_bot`).
5. Ao final, o BotFather responde com o **token**, algo assim:
   ```
   123456789:AAH9xQ-aBcDeFgHiJkLmNoPqRsTuVwXyZ12345
   ```
   👉 Esse é o valor de **`TELEGRAM_BOT_TOKEN`**. Guarde com cuidado — quem tem o token controla o bot.

### Comandos úteis do BotFather
- `/mybots` — lista seus bots e permite editar.
- `/token` — gera/revela o token de um bot existente.
- `/revoke` — invalida o token atual e gera um novo (use se vazar).

---

## 2. Reaproveitar um bot que já existe

Se você (ou a equipe) já tem um bot:

1. Abra o **@BotFather** → `/mybots` → selecione o bot.
2. Toque em **API Token** (ou envie `/token`) para ver o token.
3. Use esse token como **`TELEGRAM_BOT_TOKEN`**.

Não há problema em um mesmo bot atender vários projetos — o que separa o
destino das mensagens é o **chat_id**, não o bot.

---

## 3. Descobrir o seu `chat_id`

> ⚠️ Importante: um bot **não consegue iniciar conversa** com você.
> Você precisa **falar com o bot primeiro**, senão o envio falha com
> `403 Forbidden: bot can't initiate conversation with a user`.

### Caso A — receber no seu chat pessoal
1. Abra o seu bot (pelo username, ex.: `t.me/redeauto_pgmonitor_bot`) e clique em **Iniciar / Start** (ou envie qualquer mensagem, ex.: `oi`).
2. No navegador, acesse (troque `<TOKEN>` pelo seu token):
   ```
   https://api.telegram.org/bot<TOKEN>/getUpdates
   ```
3. Procure no JSON por `"chat":{"id": ...}`:
   ```json
   "chat": { "id": 987654321, "first_name": "Matheus", "type": "private" }
   ```
   👉 O número `987654321` é o seu **`TELEGRAM_CHAT_ID`**.

   *Alternativa rápida:* fale com **@userinfobot** no Telegram — ele responde com o seu ID.

### Caso B — receber em um grupo (recomendado para a equipe)
1. Crie um grupo e **adicione o seu bot** como membro.
2. Envie uma mensagem qualquer no grupo (ou mencione o bot).
3. Acesse novamente `https://api.telegram.org/bot<TOKEN>/getUpdates`.
4. O `chat.id` do grupo é **negativo**, ex.: `-1001234567890`. Esse é o `TELEGRAM_CHAT_ID`.

> Se o `getUpdates` vier vazio (`"result":[]`), envie uma nova mensagem ao bot/grupo e recarregue. Mensagens enviadas **antes** de o bot existir não aparecem.

---

## 4. Configurar no projeto

Copie o modelo e edite o `.env` (este arquivo **não** é versionado):

```powershell
copy .env.example .env
```

No `.env`:
```ini
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456789:AAH9xQ-aBcDeFgHiJkLmNoPqRsTuVwXyZ12345
# Opcional: um chat fixo (ex.: admin) que sempre recebe
TELEGRAM_CHAT_ID=987654321
# Senha compartilhada para auto-inscrição (veja a seção 6)
TELEGRAM_SUBSCRIBE_PASSWORD=umaSenhaForte
```

> Com a auto-inscrição (seção 6), você **não precisa** do `chat_id` fixo:
> basta as pessoas se inscreverem pelo bot com a senha. O `TELEGRAM_CHAT_ID`
> é só um destinatário extra opcional.

---

## 5. Testar

Com as dependências instaladas (`pip install -r requirements.txt`):

```powershell
# teste direto do módulo
python notifier.py

# ou com a aplicação rodando (python app.py), dispare o endpoint de teste:
curl -X POST http://localhost:5050/api/notify/test
```

Você deve receber a mensagem de teste no Telegram. Se falhar, o detalhe do erro
aparece no terminal / na resposta do endpoint.

### Erros comuns
| Erro | Causa | Solução |
|------|-------|---------|
| `401 Unauthorized` | Token errado | Confira `TELEGRAM_BOT_TOKEN` |
| `400 Bad Request: chat not found` | `chat_id` errado | Refaça o passo 3 |
| `403 ... can't initiate conversation` | Você nunca falou com o bot | Abra o bot e clique em **Start** |
| `getUpdates` vazio | Bot não recebeu mensagens | Mande mensagem e recarregue |

---

## 6. Quem recebe os status — auto-inscrição com senha

Para receber os alertas e relatórios, a pessoa precisa **se autenticar no
bot** com uma senha compartilhada. Não há tela de login no portal: a própria
conversa com o bot faz o papel de autenticação. O `chat_id` de quem se
inscreve é guardado em `subscribers.json` (arquivo local, não versionado).

### Como configurar (administrador)
Defina a senha no `.env`:
```ini
TELEGRAM_ENABLED=true
TELEGRAM_SUBSCRIBE_PASSWORD=umaSenhaForte
```
Compartilhe **a senha** (não o token!) com quem deve receber os status.
Com `app.py` rodando, o bot fica ouvindo mensagens automaticamente.

### Como cada pessoa se inscreve
1. Abre o bot no Telegram (pelo username) e clica em **Start**.
2. O bot pede a senha. A pessoa **digita a senha** e envia.
3. O bot confirma a inscrição — a partir daí ela recebe alertas e relatórios.

### Comandos disponíveis no bot
| Comando | Ação |
|---------|------|
| `/start` ou `/ajuda` | Mostra as instruções |
| *(enviar a senha)* | Inscreve o usuário |
| `/status` | Mostra o status atual da réplica (só para inscritos) |
| `/config` | Mostra suas preferências de notificação |
| `/alertas on\|off` | Liga/desliga alertas de mudança de estado |
| `/lembrete on [min] \| off` | Re-lembrete enquanto continuar crítico |
| `/relatorio off \| diario HH:MM \| intervalo Nh` | Relatório periódico (desligado, diário num horário, ou a cada N horas) |
| `/silencio HH:MM HH:MM \| off` | Horário silencioso (não perturbar) |
| `/stop` | Cancela a inscrição |

> Cada inscrito controla as **próprias** preferências por esses comandos
> (guardadas em `subscribers.json`). O horário silencioso suprime relatórios e
> re-lembretes; alertas de mudança de estado continuam sendo enviados.

> Segurança: a senha é a única barreira. Use uma senha forte, troque-a se
> vazar (basta atualizar o `.env` e reiniciar) e remova inscritos indevidos
> apagando a entrada em `subscribers.json`. Se quiser revogar tudo, troque o
> token no @BotFather (`/revoke`).
