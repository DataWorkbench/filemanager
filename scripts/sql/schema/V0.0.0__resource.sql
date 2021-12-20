CREATE
DATABASE IF NOT EXISTS data_workbench;
USE
data_workbench;
CREATE TABLE `resource`
(
    `resource_id`   VARCHAR(20) NOT NULL,
    `pid`           VARCHAR(20) NOT NULL,
    `space_id`      VARCHAR(20) NOT NULL,
    `name`          VARCHAR(500) NOT NULL,
    `type`          INT(1) DEFAULT 1 comment '1 jar 2 udf 3 connector',
    `resource_size` BIGINT(50),
    `description`   VARCHAR(5000),
    `created`       BIGINT(20) UNSIGNED NOT NULL,
    `updated`       BIGINT(20) UNSIGNED NOT NULL,
    `create_by`     VARCHAR(30) NOT NULL,
    `status`        INT(1) DEFAULT 1 comment '1 enabled 2 disabled 3 deleted',
    PRIMARY KEY (`resource_id`),
    CONSTRAINT resource_manager_chk_type check (type = 1 or type = 2 or type = 3),
    CONSTRAINT resource_manager_chk_status check (status = 1 or status = 2 or status = 3),
    INDEX resource_manager_unique (`space_id`, `type`, `name`,`status`)
);