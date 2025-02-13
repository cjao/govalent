package crud

import (
	"testing"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

func newMockAsset(key string, size int) models.AssetPublicSchema {
	return models.AssetPublicSchema{
		Key:          key,
		AssetDetails: models.AssetDetails{Size: size},
	}
}

func TestCreateAssets(t *testing.T) {
	asset_body_1 := newMockAsset(
		"/dispatch-id/node_0/function.tobj",
		5,
	)
	asset_body_2 := newMockAsset(
		"/dispatch-id/node_1/function.tobj",
		2,
	)
	config := common.NewConfigFromEnv()
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}

	created, err := CreateAssets(&config, tx, []models.AssetPublicSchema{asset_body_1, asset_body_2})
	if err != nil {
		t.Fatalf("Error inserting assets: %s\n", err.Error())
	}
	if len(created) != 2 {
		t.Fatalf("Expected to insert %d rows; actually inserted %d rows\n", 2, len(created))
	}
	ents, err := GetAssetEntitiesByPrefix(tx, "/dispatch-id", 100, 0)
	if err != nil {
		t.Fatalf("Error retrieving assets: %s\n", err.Error())
	}
	if len(ents) != 2 {
		t.Fatalf("Expected %d asset records, got %d records", 2, len(ents))
	}
}

func TestCreateGetDispatchAssetLinks(t *testing.T) {

	config := common.NewConfigFromEnv()
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}

	dispatch := newMockDispatch(nil, nil)
	err := ImportManifest(&config, tx, &dispatch)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	records, err := GetDispatchAssetLinks(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(records) != 7 {
		t.Fatalf("Expected %d records, got %d records", 7, len(records))
	}

	_, err = GetDispatchAsset(&config, tx, dispatch.Metadata.DispatchId, "result")
	if err != nil {
		t.Fatal(err.Error())
	}
	tx.Rollback()
}

func TestGetDispatchAssets(t *testing.T) {
	config := common.NewConfigFromEnv()
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}

	dispatch := newMockDispatch(nil, nil)
	err := ImportManifest(&config, tx, &dispatch)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	records, err := GetDispatchAssets(&config, tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(records) != 7 {
		t.Fatalf("Expected %d records, got %d records", 7, len(records))
	}
	tx.Rollback()
}

func TestCreateGetElectronAssetLinks(t *testing.T) {
	config := common.NewConfigFromEnv()
	d := newMockDB(t)
	// asset_body_1 := newMockAsset(
	// 	"/dispatch-id/node_0/function.tobj",
	// 	5,
	// )
	// asset_body_2 := newMockAsset(
	// 	"/dispatch-id/node_0/result.tobj",
	// 	5,
	// )
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	// CreateAssets(&config, tx, []models.AssetPublicSchema{asset_body_1, asset_body_2})

	electron := newMockElectron(0, newMockElectronMeta(0, "NEW_OBJECT"), models.ElectronAssets{})
	dispatch := newMockDispatch([]models.ElectronSchema{electron}, nil)
	err := ImportManifest(&config, tx, &dispatch)
	if err != nil {
		t.Fatal(err.Error())
	}

	links, err := GetElectronAssetLinks(tx, dispatch.Metadata.DispatchId, electron.NodeId)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(links) != 8 {
		t.Fatalf("Expected %d electron-asset links; got %d links", 8, len(links))
	}

	tx.Rollback()
}

func TestGetElectronAssets(t *testing.T) {
	config := common.NewConfigFromEnv()
	d := newMockDB(t)
	// asset_body_1 := newMockAsset(
	// 	"/dispatch-id/node_0/function.tobj",
	// 	5,
	// )
	// asset_body_2 := newMockAsset(
	// 	"/dispatch-id/node_0/result.tobj",
	// 	5,
	// )
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	// CreateAssets(&config, tx, []models.AssetPublicSchema{asset_body_1, asset_body_2})

	electron := newMockElectron(0, newMockElectronMeta(0, "NEW_OBJECT"), models.ElectronAssets{})
	dispatch := newMockDispatch([]models.ElectronSchema{electron}, nil)
	err := ImportManifest(&config, tx, &dispatch)
	if err != nil {
		t.Fatal(err.Error())
	}

	assets, err := GetElectronAssets(&config, tx, dispatch.Metadata.DispatchId, electron.NodeId)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(assets) != 8 {
		t.Fatalf("Expected %d electron-asset links; got %d assets", 8, len(assets))
	}

	tx.Rollback()
}
