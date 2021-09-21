CREATE TABLE `resource`
(
    `id`           VARCHAR(20)         NOT NULL,
    `pid`          VARCHAR(20)         NOT NULL,
    `space_id`     VARCHAR(20)         NOT NULL,
    `name`         VARCHAR(60)         NOT NULL,
    `type`         INT(1) DEFAULT 1 comment '1 jar 2 udf 3 connector',
    `size`         BIGINT(20),
    `created`      BIGINT(20) UNSIGNED NOT NULL,
    `updated`      BIGINT(20) UNSIGNED NOT NULL,
    PRIMARY KEY (`id`),
    CONSTRAINT resource_manager_chk_type check (type = 1 or type = 2 or type = 3),
    UNIQUE INDEX resource_manager_unique (`space_id`, `name`,`type`)
);