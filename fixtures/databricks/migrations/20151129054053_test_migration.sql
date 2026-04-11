-- migrate:up
CREATE TABLE users (
  id BIGINT,
  name STRING
);

-- migrate:down
DROP TABLE users;
