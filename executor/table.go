package executor

var (
	FileManagerTableName = "file_manager"
	FileDirTableName     = "file_dir"
)

type FileManager struct {
	ID              string `gorm:"column:id;primaryKey;type:varchar(25);not null" json:"id"`
	Name            string `gorm:"column:name;type:varchar(25);not null;index:file_manager_unique,unique" json:"name"`
	FileDirID       string `gorm:"column:dir_id;type:varchar(25);not null;index:file_manager_unique,unique" json:"-"`
	FileType        int32  `gorm:"column:file_type;type:int(1);default:1" json:"file_type"`
	CreateTime      string `gorm:"column:create_time;type:varchar(20)" json:"-"`
	UpdateTime      string `gorm:"column:update_time;type:varchar(20)" json:"-"`
	DeleteTimestamp int32  `gorm:"column:delete_timestamp;index:file_manager_unique,unique" json:"-"`
}

type FileDir struct {
	ID              string        `gorm:"column:id;primaryKey;type:varchar(25);not null" json:"-"`
	Address         string        `gorm:"column:address;type:varchar(200);not null;index:file_dir_unique,unique" json:"-"`
	SpaceID         string        `gorm:"column:space_id;type:varchar(25);not null;index:file_dir_unique,unique" json:"-"`
	Url             string        `gorm:"column:url;type:varchar(500);not null;index:file_dir_unique,unique" json:"-"`
	Name            string        `gorm:"column:name;type:varchar(200);not null" json:"name"`
	Level           int32         `gorm:"column:level;type:int(1);default:1" json:"level"`
	ParentID        *string       `gorm:"column:parent;type:varchar(25)" json:"-"`
	SubDir          []*FileDir    `gorm:"foreignKey:ParentID;references:ID" json:"sub_dir"`
	SubFile         []FileManager `json:"sub_file"`
	CreateTime      string        `gorm:"column:create_time;type:varchar(20)" json:"-"`
	UpdateTime      string        `gorm:"column:update_time;type:varchar(20)" json:"-"`
	DeleteTimestamp int32         `gorm:"column:delete_timestamp;index:file_dir_unique,unique" json:"-"`
}

func (f FileManager) TableName() string {
	return FileManagerTableName
}
func (f FileDir) TableName() string {
	return FileDirTableName
}
