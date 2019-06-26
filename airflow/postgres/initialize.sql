CREATE TABLE sample_output (
    -- ds: To allow idempotence we need track ds (execution date)
    ds VARCHAR(16),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    rua VARCHAR,
    numero VARCHAR,
    bairro VARCHAR,
    cidade VARCHAR,
    cep VARCHAR,
    estado VARCHAR,
    pais VARCHAR
);

CREATE INDEX ds_index ON sample_output(ds);