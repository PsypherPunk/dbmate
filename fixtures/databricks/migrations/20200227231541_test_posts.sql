-- migrate:up
CREATE TABLE posts (
  id BIGINT,
  title STRING
);

-- migrate:down
DROP TABLE posts;
