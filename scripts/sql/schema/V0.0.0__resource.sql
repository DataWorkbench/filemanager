CREATE TABLE `resource`
(
    -- Resource ID, unique within a region
    `id`           VARCHAR(20)         NOT NULL,
    -- Resource Parent ID
    `pid`          VARCHAR(20),
    `space_id`     VARCHAR(20)         NOT NULL,
    `name`         VARCHAR(60)         NOT NULL,
    `type`         INT(1) DEFAULT 1 comment '1 jar 2 udf',
    `size`         BIGINT(20),
    `is_directory` BOOL,
    `created`      BIGINT(20) UNSIGNED NOT NULL,
    `updated`      BIGINT(20) UNSIGNED NOT NULL,
    PRIMARY KEY (`id`),
    CONSTRAINT file_manager_chk_type check (type = 0 or type = 1 or type = 2),
    UNIQUE INDEX file_manager_unique (`pid`, `space_id`, `name`)
);