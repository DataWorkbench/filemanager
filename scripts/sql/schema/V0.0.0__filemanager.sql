CREATE TABLE `file_dir`
(
    `id`               varchar(25)  NOT NULL,
    `address`          varchar(200) NOT NULL,
    `space_id`         varchar(25)  NOT NULL,
    `url`              varchar(500) NOT NULL,
    `name`             varchar(200) NOT NULL,
    `level`            int(1) DEFAULT 1,
    `parent`           varchar(25),
    `create_time`      varchar(20),
    `update_time`      varchar(20),
    `delete_timestamp` int(13),
    PRIMARY KEY (`id`),
    UNIQUE INDEX file_dir_unique (`address`, `space_id`, `url`, `delete_timestamp`),
    CONSTRAINT `fk_file_dir_sub_dir` FOREIGN KEY (`parent`) REFERENCES `file_dir` (`id`)
);

CREATE TABLE `file_manager`
(
    `id`               varchar(25) NOT NULL,
    `name`             varchar(25) NOT NULL,
    `dir_id`           varchar(25) NOT NULL,
    `file_type`        int(1) DEFAULT 1,
    `create_time`      varchar(20),
    `update_time`      varchar(20),
    `delete_timestamp` int(13),
    PRIMARY KEY (`id`),
    UNIQUE INDEX file_manager_unique (`name`, `dir_id`, `delete_timestamp`),
    CONSTRAINT `fk_file_dir_sub_file` FOREIGN KEY (`dir_id`) REFERENCES `file_dir` (`id`)
)


