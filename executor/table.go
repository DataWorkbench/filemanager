package executor

import "time"

var (
	FileManagerTableName = "file_manager"
)

type FileManager struct {
	ID              string    `gorm:"column:id;primaryKey;type:varchar(20);not null" json:"id"`
	SpaceID         string    `gorm:"column:space_id;not null" json:"space_id"`
	VirtualPath     string    `gorm:"column:virtual_path;type:varchar(250);not null" json:"virtual_path"`
	VirtualName     string    `gorm:"column:virtual_name;type:varchar(60);not null" json:"virtual_name"`
	Type            int32     `gorm:"column:type;type:int(1)" json:"type"`
	IsDir           bool      `gorm:"column:is_dir" json:"is_dir"`
	CreatedAt       time.Time `gorm:"column:create_time" json:"-"`
	UpdatedAt       time.Time `gorm:"column:update_time" json:"-"`
	DeleteTimestamp *int32    `gorm:"column:delete_timestamp" json:"-"`
}

func (f FileManager) TableName() string {
	return FileManagerTableName
}
