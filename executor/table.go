package executor

var (
	FileManagerTableName = "file_manager"
)

type FileManager struct {
	ID              string `gorm:"column:id;primaryKey;type:varchar(20);not null" json:"id"`
	SpaceID         string `gorm:"column:space_id;not null" json:"space_id"`
	HdfsPath        string `gorm:"column:hdfs_path;type:varchar(250);not null" json:"hdfs_path"`
	VirtualPath     string `gorm:"column:virtual_path;type:varchar(250);not null" json:"virtual_path"`
	VirtualName     string `gorm:"column:virtual_name;type:varchar(60);not null" json:"virtual_name"`
	Type            int32  `gorm:"column:type;type:int(1)" json:"type"`
	CreateTime      string `gorm:"column:create_time;type:varchar(20)" json:"-"`
	UpdateTime      string `gorm:"column:update_time;type:varchar(20)" json:"-"`
	DeleteTimestamp *int32 `gorm:"column:delete_timestamp" json:"-"`
}

func (f FileManager) TableName() string {
	return FileManagerTableName
}
