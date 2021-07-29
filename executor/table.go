package executor

var (
	FileManagerTableName = "file_manager"
)

type FileManager struct {
	ID              string `gorm:"column:id;primaryKey;type:varchar(25);not null" json:"id"`
	SpaceID         string `gorm:"column:space_id;not null" json:"space_id"`
	Name            string `gorm:"column:name;type:varchar(25);not null" json:"name"`
	HdfsName        string `gorm:"column:hdfs_name;type:varchar(25);not null" json:"hdfs_name"`
	Path            string `gorm:"column:path;type:varchar(25);not null" json:"path"`
	Type            int32  `gorm:"column:type;type:int(1)" json:"type"`
	Address         string `gorm:"column:address" json:"address"`
	CreateTime      string `gorm:"column:create_time;type:varchar(20)" json:"-"`
	UpdateTime      string `gorm:"column:update_time;type:varchar(20)" json:"-"`
	DeleteTimestamp *int32 `gorm:"column:delete_timestamp" json:"-"`
}

func (f FileManager) TableName() string {
	return FileManagerTableName
}
