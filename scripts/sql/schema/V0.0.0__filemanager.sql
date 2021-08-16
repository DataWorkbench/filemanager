CREATE TABLE `file_manager`
(
    `id`               varchar(25)  NOT NULL,
    `space_id`         varchar(20)  NOT NULL,
    `name`             varchar(25)  NOT NULL,
    `hdfs_path`        varchar(500) NOT NULL,
    `path`             varchar(25)  NOT NULL,
    `type`             int(1) DEFAULT 1 comment '1 jar 2 udf',
    `address`          varchar(20),
    `create_time`      varchar(20),
    `update_time`      varchar(20),
    `delete_timestamp` int(13),
    PRIMARY KEY (`id`),
    CONSTRAINT file_manager_chk_type check (type = 1 or type = 2),
    UNIQUE INDEX file_manager_unique (`address`, `space_id`, `path`, `name`, `delete_timestamp`)
);
