package executor

import "time"

var (
	ResourceManagerTableName = "resource"
)

type Resource struct {
	ResourceId string `gorm:"column:id;primaryKey;type:varchar(20);not null" json:"id"`
	ParentId   string `gorm:"column:pid;type:varchar(20);not null" json:"pid"`
	SpaceId    string `gorm:"column:space_id;not null" json:"space_id"`
	Name       string `gorm:"column:name;type:varchar(250);not null" json:"name"`
	Type       int32  `gorm:"column:type;type:int(1)" json:"type"`
	Size       int64  `gorm:"column:size;type:bigint(20)" json:"size"`
	IsDir      bool   `gorm:"column:is_directory" json:"is_directory"`
	CreatedAt  time.Time `gorm:"column:create_time" json:"-"`
	UpdatedAt  time.Time `gorm:"column:update_time" json:"-"`
}

func (r Resource) TableName() string {
	return ResourceManagerTableName
}

func (r Resource) CheckParams() bool {
	return r.ResourceId != "" && r.ParentId != "" && r.SpaceId != "" && r.Name != "" && r.Size != 0
}
