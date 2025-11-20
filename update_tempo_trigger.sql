
Alter table relatorio_projetos_historico add COLUMN tempo BIGINT;

CREATE OR REPLACE FUNCTION calc_tempo_em_segundos()
RETURNS trigger AS $$
BEGIN
    NEW.tempo :=
        COALESCE( (regexp_match(NEW.duracao, '(\d+)\s*Dia'))[1]::int, 0 ) * 86400 +
        COALESCE( (regexp_match(NEW.duracao, '(\d+)\s*Hr'))[1]::int, 0 ) * 3600 +
        COALESCE( (regexp_match(NEW.duracao, '(\d+)\s*Min'))[1]::int, 0 ) * 60 +
        COALESCE( (regexp_match(NEW.duracao, '(\d+)\s*Seg'))[1]::int, 0 );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS trg_update_tempo ON relatorio_projetos_historico;

CREATE TRIGGER trg_update_tempo
BEFORE INSERT OR UPDATE OF duracao
ON relatorio_projetos_historico
FOR EACH ROW
EXECUTE FUNCTION calc_tempo_em_segundos();

UPDATE relatorio_projetos_historico set duracao = duracao;