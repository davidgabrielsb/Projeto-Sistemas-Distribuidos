PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS eventos(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,tabela TEXT NOT NULL,dados TEXT NOT NULL);

CREATE TRIGGER IF NOT EXISTS registrar_logins AFTER INSERT ON logins
WHEN NEW.evento_id IS NULL
BEGIN
    UPDATE logins SET evento_id=lower(hex(randomblob(16))) WHERE id=NEW.id;
    INSERT INTO eventos(id,tabela,dados)
    SELECT evento_id,'logins',json_object('usuario',usuario,'timestamp',timestamp) FROM logins WHERE id=NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS registrar_canais AFTER INSERT ON canais
WHEN NEW.evento_id IS NULL
BEGIN
    UPDATE canais SET evento_id=lower(hex(randomblob(16))) WHERE id=NEW.id;
    INSERT INTO eventos(id,tabela,dados)
    SELECT evento_id,'canais',json_object('nome',nome,'usuario',usuario,'timestamp',timestamp) FROM canais WHERE id=NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS registrar_mensagens AFTER INSERT ON mensagens
WHEN NEW.evento_id IS NULL
BEGIN
    UPDATE mensagens SET evento_id=lower(hex(randomblob(16))) WHERE id=NEW.id;
    INSERT INTO eventos(id,tabela,dados)
    SELECT evento_id,'mensagens',json_object('canal',canal,'usuario',usuario,'mensagem',mensagem,'timestamp_envio',timestamp_envio,'timestamp_publicacao',timestamp_publicacao) FROM mensagens WHERE id=NEW.id;
END;
