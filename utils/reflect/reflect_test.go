package reflect

import (
	"reflect"
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/test/assert"
)

// MockComponent a mock component for testing
type MockComponent struct {
	Config MockComponentConfig
	Name   string
}

func (mc *MockComponent) Type() string { return "mock" }
func (mc *MockComponent) New() types.Node {
	return &MockComponent{Config: MockComponentConfig{Field1: "default1", FieldInt: 10, FieldBool: true, FieldSlice: []string{"s1", "s2"}, FieldMap: map[string]string{"mk1": "mv1"}, Nested: NestedConfig{NestedField: "nestedDefault"}}}}
func (mc *MockComponent) Init(types.Config, types.Configuration) error   { return nil }
func (mc *MockComponent) OnMsg(types.RuleContext, types.RuleMsg) error { return nil }
func (mc *MockComponent) Destroy()                                       {}

// MockComponentConfig a mock component config for testing
type MockComponentConfig struct {
	Field1     string            `label:"Field One" desc:"Description for field one" required:"true"`
	FieldInt   int               `label:"Field Int" desc:"Description for int field"`
	FieldBool  bool              `label:"Field Bool"`
	FieldSlice []string          `label:"Field Slice"`
	FieldMap   map[string]string `label:"Field Map"`
	Nested     NestedConfig      `label:"Nested Config"`
	NoTagField string
}

type NestedConfig struct {
	NestedField string `label:"Nested Field" desc:"A nested field" required:"false"`
}

// MockComponentWithDef a mock component implementing ComponentDefGetter
type MockComponentWithDef struct {
	MockComponent
}

func (mcwd *MockComponentWithDef) Def() types.ComponentForm {
	return types.ComponentForm{
		Label:         "Custom Label",
		Type:          "customMockType",
		Category:      "customCategory",
		Desc:          "Custom Description",
		Fields:        []types.ComponentFormField{{Name: "customField", Type: "string"}},
		RelationTypes: &[]string{types.Success, "CustomRelation"},
		Version:       "2.0",
		ComponentKind: types.ComponentKindPlugin,
		Disabled:      true,
	}
}

// MockComponentWithCategory a mock component implementing CategoryGetter
type MockComponentWithCategory struct {
	MockComponent
}

func (mcc *MockComponentWithCategory) Category() string {
	return "specificCategory"
}

// MockComponentWithDesc a mock component implementing DescGetter
type MockComponentWithDesc struct {
	MockComponent
}

func (mcd *MockComponentWithDesc) Desc() string {
	return "A very specific description"
}

// MockEndpointComponent a mock endpoint component
type MockEndpointComponent struct {
	MockComponent
	endpoint.BaseEndpoint
}

func (mec *MockEndpointComponent) Type() string { return "mockEndpoint" }
func (mec *MockComponent) NewEndpoint() types.Node {
	return &MockEndpointComponent{}
}

func TestGetComponentConfig(t *testing.T) {
	mc := &MockComponent{}
	mc = mc.New().(*MockComponent)

	compType, configField, configValue := GetComponentConfig(mc)
	assert.Equal(t, "MockComponent", compType.Name())
	assert.Equal(t, "Config", configField.Name)
	assert.NotNil(t, configValue)
	assert.Equal(t, "MockComponentConfig", configField.Type.Name())

	// Test with component that has lowercase 'config'
	type MockComponentLowerConfig struct {
		config MockComponentConfig
	}
	lowerMc := &MockComponentLowerConfig{config: MockComponentConfig{Field1: "val"}}
	compTypeLower, configFieldLower, configValueLower := GetComponentConfig(lowerMc)
	assert.Equal(t, "MockComponentLowerConfig", compTypeLower.Name())
	assert.Equal(t, "config", configFieldLower.Name)
	assert.NotNil(t, configValueLower)
	assert.Equal(t, "MockComponentConfig", configFieldLower.Type.Name())

	// Test with component that has no 'Config' or 'config' field
	type MockComponentNoConfig struct {
		OtherField string
	}
	noCfgMc := &MockComponentNoConfig{}
	_, configFieldNoCfg, _ := GetComponentConfig(noCfgMc)
	assert.Equal(t, "", configFieldNoCfg.Name) // Expect empty field name
}

func TestGetFields(t *testing.T) {
	mc := &MockComponent{}
	mc = mc.New().(*MockComponent)
	_, configField, configValue := GetComponentConfig(mc)

	fields := GetFields(configField, configValue)
	assert.Equal(t, 7, len(fields))

	// Check Field1
	field1 := fields[0]
	assert.Equal(t, "field1", field1.Name)
	assert.Equal(t, "string", field1.Type)
	assert.Equal(t, "default1", field1.DefaultValue)
	assert.Equal(t, "Field One", field1.Label)
	assert.Equal(t, "Description for field one", field1.Desc)
	assert.True(t, field1.Rules[0]["required"].(bool))

	// Check FieldInt
	fieldInt := fields[1]
	assert.Equal(t, "fieldInt", fieldInt.Name)
	assert.Equal(t, "int", fieldInt.Type)
	assert.Equal(t, 10, fieldInt.DefaultValue)
	assert.Equal(t, "Field Int", fieldInt.Label)

	// Check FieldSlice
	fieldSlice := fields[3]
	assert.Equal(t, "fieldSlice", fieldSlice.Name)
	assert.Equal(t, "array", fieldSlice.Type)
	assert.Equal(t, []string{"s1", "s2"}, fieldSlice.DefaultValue)

	// Check FieldMap
	fieldMap := fields[4]
	assert.Equal(t, "fieldMap", fieldMap.Name)
	assert.Equal(t, "map", fieldMap.Type)
	assert.Equal(t, map[string]string{"mk1": "mv1"}, fieldMap.DefaultValue)

	// Check Nested struct
	nestedField := fields[5]
	assert.Equal(t, "nested", nestedField.Name)
	assert.Equal(t, "struct", nestedField.Type)
	assert.Equal(t, "Nested Config", nestedField.Label)
	assert.Equal(t, 1, len(nestedField.Fields))
	assert.Equal(t, "nestedField", nestedField.Fields[0].Name)
	assert.Equal(t, "Nested Field", nestedField.Fields[0].Label)
	assert.Equal(t, "nestedDefault", nestedField.Fields[0].DefaultValue)

	// Check NoTagField
	noTagField := fields[6]
	assert.Equal(t, "noTagField", noTagField.Name)
	assert.Equal(t, "string", noTagField.Type)
	assert.Equal(t, "", noTagField.Label)
	assert.Equal(t, "", noTagField.Desc)
	assert.Equal(t, nil, noTagField.Rules) // No required tag
}

func TestGetComponentForm(t *testing.T) {
	// Test with basic MockComponent
	mc := &MockComponent{}
	mc = mc.New().(*MockComponent)
	form := GetComponentForm(mc)

	assert.Equal(t, "MockComponent", form.Label)
	assert.Equal(t, "mock", form.Type)
	assert.True(t, len(form.Fields) > 0)
	assert.Equal(t, types.ComponentKindNative, form.ComponentKind)
	assert.Contains(t, *form.RelationTypes, types.Success)
	assert.Contains(t, *form.RelationTypes, types.Failure)

	// Test with MockComponentWithDef (overrides)
	mcwd := &MockComponentWithDef{}
	mcwd.MockComponent = *mcwd.New().(*MockComponent) // Initialize inner mock
	formDef := GetComponentForm(mcwd)

	assert.Equal(t, "Custom Label", formDef.Label)
	assert.Equal(t, "customMockType", formDef.Type)
	assert.Equal(t, "customCategory", formDef.Category)
	assert.Equal(t, "Custom Description", formDef.Desc)
	assert.Equal(t, 1, len(formDef.Fields))
	assert.Equal(t, "customField", formDef.Fields[0].Name)
	assert.Contains(t, *formDef.RelationTypes, "CustomRelation")
	assert.Equal(t, "2.0", formDef.Version)
	assert.Equal(t, types.ComponentKindPlugin, formDef.ComponentKind)
	assert.True(t, formDef.Disabled)

	// Test with MockComponentWithCategory
	mcc := &MockComponentWithCategory{}
	mcc.MockComponent = *mcc.New().(*MockComponent)
	formCat := GetComponentForm(mcc)
	assert.Equal(t, "specificCategory", formCat.Category)

	// Test with MockComponentWithDesc
	mcd := &MockComponentWithDesc{}
	mcd.MockComponent = *mcd.New().(*MockComponent)
	formDesc := GetComponentForm(mcd)
	assert.Equal(t, "A very specific description", formDesc.Desc)

	// Test with MockEndpointComponent
	mec := &MockEndpointComponent{}
	mec.MockComponent = *mec.New().(*MockComponent)
	formEndpoint := GetComponentForm(mec)
	assert.Equal(t, types.ComponentKindEndpoint, formEndpoint.ComponentKind)
	assert.Equal(t, 0, len(*formEndpoint.RelationTypes)) // Endpoints usually have no predefined relation types in this context

	// Test filter component relation types
	type MockFilterComponent struct{ MockComponent }

	func (mfc *MockFilterComponent) Type() string { return "filter" }
	func (mfc *MockFilterComponent) New() types.Node {
		return &MockFilterComponent{MockComponent{Name: "Test Filter"}}
	}
	mfc := (&MockFilterComponent{}).New().(*MockFilterComponent)
	formFilter := GetComponentForm(mfc)
	assert.Equal(t, "Test Filter", formFilter.Label) // Name is used for Label if not overridden
	assert.Contains(t, *formFilter.RelationTypes, types.True)
	assert.Contains(t, *formFilter.RelationTypes, types.False)
	assert.Contains(t, *formFilter.RelationTypes, types.Failure)
	assert.Equal(t, 3, len(*formFilter.RelationTypes))

	// Test switch component relation types
	type MockSwitchComponent struct{ MockComponent }

	func (msc *MockSwitchComponent) Type() string { return "switch" }
	func (msc *MockSwitchComponent) New() types.Node {
		return &MockSwitchComponent{MockComponent{Name: "Test Switch"}}
	}
	msc := (&MockSwitchComponent{}).New().(*MockSwitchComponent)
	formSwitch := GetComponentForm(msc)
	assert.Equal(t, "Test Switch", formSwitch.Label)
	assert.Equal(t, 0, len(*formSwitch.RelationTypes))

	// Test iterator component relation types
	type MockIteratorComponent struct{ MockComponent }

	func (mic *MockIteratorComponent) Type() string { return "iterator" }
	func (mic *MockIteratorComponent) New() types.Node {
		return &MockIteratorComponent{MockComponent{Name: "Test Iterator"}}
	}
	mic := (&MockIteratorComponent{}).New().(*MockIteratorComponent)
	formIterator := GetComponentForm(mic)
	assert.Equal(t, "Test Iterator", formIterator.Label)
	assert.Contains(t, *formIterator.RelationTypes, types.True)
	assert.Contains(t, *formIterator.RelationTypes, types.False)
	assert.Contains(t, *formIterator.RelationTypes, types.Success)
	assert.Contains(t, *formIterator.RelationTypes, types.Failure)
	assert.Equal(t, 4, len(*formIterator.RelationTypes))

}

func TestCoverComponentForm(t *testing.T) {
	fromDef := types.ComponentForm{
		Label:         "New Label",
		Type:          "newType",
		Category:      "newCategory",
		Desc:          "New Description",
		Fields:        []types.ComponentFormField{{Name: "newField"}},
		RelationTypes: &[]string{"Rel1"},
		Version:       "3.0",
		ComponentKind: types.ComponentKindFlow,
		Disabled:      true,
	}
	toForm := types.ComponentForm{
		Label:         "Old Label",
		Type:          "oldType",
		Category:      "oldCategory",
		Desc:          "Old Description",
		Fields:        []types.ComponentFormField{{Name: "oldField"}},
		RelationTypes: &[]string{"OldRel"},
		Version:       "1.0",
		ComponentKind: types.ComponentKindNative,
		Disabled:      false,
	}

	mockGetter := &MockComponentWithDef{}
	// Simulate the Def() method returning fromDef
	originalDefFunc := mockGetter.Def
	mockGetter.Def = func() types.ComponentForm { return fromDef }
	defer func() { mockGetter.Def = originalDefFunc }() // Restore original

	resultForm := coverComponentForm(mockGetter, toForm)

	assert.Equal(t, fromDef.Label, resultForm.Label)
	assert.Equal(t, fromDef.Type, resultForm.Type)
	assert.Equal(t, fromDef.Category, resultForm.Category)
	assert.Equal(t, fromDef.Desc, resultForm.Desc)
	assert.Equal(t, fromDef.Fields, resultForm.Fields)
	assert.Equal(t, fromDef.RelationTypes, resultForm.RelationTypes)
	assert.Equal(t, fromDef.Version, resultForm.Version)
	assert.Equal(t, fromDef.ComponentKind, resultForm.ComponentKind)
	assert.Equal(t, fromDef.Disabled, resultForm.Disabled)

	// Test with empty fields in fromDef to ensure they don't overwrite existing if not provided
	fromDefPartial := types.ComponentForm{
		Label: "Partial Label", // Only label is set
	}
	mockGetter.Def = func() types.ComponentForm { return fromDefPartial }
	resultFormPartial := coverComponentForm(mockGetter, toForm)
	assert.Equal(t, "Partial Label", resultFormPartial.Label)
	assert.Equal(t, toForm.Type, resultFormPartial.Type) // Should remain oldType
}