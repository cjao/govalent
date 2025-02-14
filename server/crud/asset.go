package crud

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
)

const STORAGE_SCHEME = "file"

type AssetEntity struct {
	public *models.AssetPublicSchema

	// Internal attributes

	// Md5 hash of key
	id string

	// internal url scheme; only "file" (local to the server) is currently supported
	scheme string

	base_path string
}

func NewAssetEntity() AssetEntity {
	return AssetEntity{
		public: &models.AssetPublicSchema{},
	}
}

func ComputeAssetId(key string) string {
	hasher := md5.New()
	hash := make([]byte, 0)
	io.WriteString(hasher, key)
	return hex.EncodeToString(hasher.Sum(hash))
}

func NewAssetEntityFromPublic(c *common.Config, public *models.AssetPublicSchema) AssetEntity {
	id := ComputeAssetId(public.Key)
	scheme := STORAGE_SCHEME
	base_path := c.StoragePath
	return AssetEntity{
		public:    public,
		id:        id,
		scheme:    scheme,
		base_path: base_path,
	}
}

func (a *AssetEntity) Fields() []string {
	return []string{
		db.ASSET_TABLE_ID,
		db.ASSET_TABLE_SCHEME,
		db.ASSET_TABLE_BASE,
		db.ASSET_TABLE_KEY,
		db.ASSET_TABLE_SIZE,
		db.ASSET_TABLE_DIGEST,
		db.ASSET_TABLE_DIGEST_ALG,
		db.ASSET_TABLE_REMOTE_URI,
	}
}

func (a *AssetEntity) Values() []any {
	return []any{
		a.id,
		a.scheme,
		a.base_path,
		a.public.Key,
		a.public.Size,
		a.public.DigestAlg,
		a.public.Digest,
		a.public.Uri,
	}
}

func (a *AssetEntity) Fieldrefs() []any {
	return []any{
		&a.id,
		&a.scheme,
		&a.base_path,
		&a.public.Key,
		&a.public.Size,
		&a.public.DigestAlg,
		&a.public.Digest,
		&a.public.Uri,
	}
}

// Internal URI
func (a *AssetEntity) getURI() string {
	return fmt.Sprintf("%s://%s/%s", a.scheme, a.base_path, a.public.Key)
}

// TODO: support URI filtering
// TODO: distinguish by direction (upload/download)
func (a *AssetEntity) GetPublicUri(c *common.Config) string {
	return a.getURI()
}

// For use by GET endpoint
// This mutates a
func (a *AssetEntity) GetPublicEntity(c *common.Config) *models.AssetPublicSchema {
	if a.public.Size > 0 {
		a.public.RemoteUri = a.GetPublicUri(c)
	}
	return a.public
}

// Register assets and populate each manifest's RemoteURI with the asset's
// upload URL
func CreateAssets(c *common.Config, t *sql.Tx, a []models.AssetPublicSchema) ([]AssetEntity, *models.APIError) {
	ents := make([]AssetEntity, len(a))
	for i := range a {
		ent := NewAssetEntityFromPublic(c, &a[i])
		ents[i] = ent
	}
	_, err := createAssetsFromEntities(t, ents)
	if err != nil {
		return nil, err
	}

	// Only non-null assets will be uploaded
	for _, ent := range ents {
		if ent.public.Size > 0 && len(ent.public.Uri) == 0 {
			ent.public.RemoteUri = ent.GetPublicUri(c)
			slog.Debug(fmt.Sprintf("Returning upload URI for asset: %s\n", ent.public.RemoteUri))
		}
	}
	return ents, nil
}

func createAssetsFromEntities(t *sql.Tx, a []AssetEntity) (int, *models.APIError) {
	if len(a) == 0 {
		return 0, nil
	}
	// TODO: cache this template
	template, _ := generateInsertTemplate(db.ASSET_TABLE, (&a[0]).Fields())

	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return 0, models.NewGenericServerError(err)
	}
	for i := 0; i < len(a); i++ {
		_, err = stmt.Exec((&a[i]).Values()...)
		if err != nil {
			return i, models.NewGenericServerError(err)
		}
	}
	slog.Debug(fmt.Sprintf("Inserted %d asset records\n", len(a)))
	return len(a), nil
}

func GetAssetEntitiesByPrefix(t *sql.Tx, prefix string, limit int, offset int) ([]AssetEntity, *models.APIError) {

	results := make([]AssetEntity, 0)
	f := Filters{}
	(&f).AddLike(db.ASSET_TABLE_KEY, fmt.Sprintf("%s%%", prefix))

	template := generateSelectTemplate(
		db.ASSET_TABLE,
		(&AssetEntity{}).Fields(),
		f.RenderTemplate(),
		db.ASSET_TABLE_KEY,
		true,
		true,
	)
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Error(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}

	params := (&f).RenderValues()
	params = append(params, limit)
	params = append(params, offset)
	rows, err := stmt.Query(params...)
	if err != nil {
		slog.Error(fmt.Sprintf("Error executing query: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}

	for rows.Next() {
		ent := AssetEntity{public: &models.AssetPublicSchema{}}
		err = rows.Scan((&ent).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		results = append(results, ent)
	}

	return results, nil
}
