%%sql

CREATE TABLE `third-essence-345723.temp.customer`

(
    id STRING,                      -- Unikalny identyfikator UUID
    first_name STRING,              -- Imię
    last_name STRING,               -- Nazwisko
    email STRING,                   -- Adres email
    phone_number STRING,            -- Numer telefonu
    address STRING,                 -- Adres
    city STRING,                    -- Miasto
    state STRING,                   -- Stan
    zip_code STRING,                -- Kod pocztowy
    country STRING,                 -- Kraj
    birthdate DATE,                 -- Data urodzenia
    gender STRING                   -- Płeć (Male, Female, Other)
)