CREATE TABLE `file_manager`
(
    `id`               varchar(20)  NOT NULL,
    `space_id`         varchar(20)  NOT NULL,
    `virtual_path`     varchar(250) NOT NULL,
    `virtual_name`     varchar(60)  NOT NULL,
    `type`             int(1)   DEFAULT 1 comment '1 jar 2 udf',
    `create_time`      datetime,
    `update_time`      datetime default now(),
    `delete_timestamp` int(13),
    `is_dir`           bool,
    PRIMARY KEY (`id`),
    CONSTRAINT file_manager_chk_type check (type = 0 or type = 1 or type = 2),
    UNIQUE INDEX file_manager_unique (`space_id`, `virtual_path`, `virtual_name`, `delete_timestamp`, `is_dir`)
);