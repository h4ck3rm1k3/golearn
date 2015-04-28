package base

import (
	"bytes"
	"fmt"
	//"os"
//	"sync"
)

/* data interface delegates the storage to another module
 attribute groups are sets of attributes treated together. 
for example binary attributes are grouped together to save bytes. 
Also the attributes can have the same type or size in one group.
in captnproto or protobuf this should be managed transparently.
*/

type DataInterface struct {
	agMap        map[string]int
	agRevMap     map[int]string
	//ags          []AttributeGroup
	//	lock         sync.Mutex
	fixed        bool
	classAttrs   map[AttributeSpec]bool
	maxRow       int
	attributes   []Attribute
	tmpAttrAgMap map[Attribute]string
	// Counters for each AttributeGroup type
	floatRowSizeBytes int
	catRowSizeBytes   int
	binRowSizeBits    int
}

// NewDataInterface generates a new DataInterface set
// with an anonymous EDF mapping and default settings.
func NewDataInterface() *DataInterface {
	return &DataInterface{
		make(map[string]int),
		make(map[int]string),
		//make([]AttributeGroup, 0),
		//sync.Mutex{},
		false,
		make(map[AttributeSpec]bool),
		0,
		make([]Attribute, 0),
		make(map[Attribute]string),
		0,
		0,
		0,
	}
}

// NewDataInterfaceCopy generates a new DataInterface set
// from an existing FixedDataGrid.
func NewDataInterfaceCopy(of FixedDataGrid) *DataInterface {

	ret := NewDataInterface() // Create the skeleton
	// Attribute creation
	attrs := of.AllAttributes()
	specs1 := make([]AttributeSpec, len(attrs))
	specs2 := make([]AttributeSpec, len(attrs))
	for i, a := range attrs {
		// Retrieve old AttributeSpec
		s, err := of.GetAttribute(a)
		if err != nil {
			panic(err)
		}
		specs1[i] = s
		// Add and store new AttributeSpec
		specs2[i] = ret.AddAttribute(a)
	}
	// Allocate memory
	_, rows := of.Size()
	ret.Extend(rows)

	// Copy each row from the old one to the new
	of.MapOverRows(specs1, func(v [][]byte, r int) (bool, error) {
		for i, c := range v {
			ret.Set(specs2[i], r, c)
		}
		return true, nil
	})

	return ret
}


// CreateAttributeGroup adds a new AttributeGroup to this set of instances
// with a given name. If the size is 0, a bit-ag is added
// if the size of not 0, then the size of each ag attribute
// is set to that number of bytes.
func (inst *DataInterface) CreateAttributeGroup(name string, size int) (err error) {
	return nil
}

// AllAttributeGroups returns a copy of the available AttributeGroups
func (inst *DataInterface) AllAttributeGroups() map[string]AttributeGroup {
	ret := make(map[string]AttributeGroup)
//	for a := range inst.agMap {
		//ret[a] = inst.ags[inst.agMap[a]]
//	}
	return ret
}

// GetAttributeGroup returns a reference to a AttributeGroup of a given name /
func (inst *DataInterface) GetAttributeGroup(name string) (AttributeGroup, error) {
	return nil,nil
}

//
// Attribute creation and handling
//

// AddAttribute adds an Attribute to this set of DataInterface
// Creates a default AttributeGroup for it if a suitable one doesn't exist.
// Returns an AttributeSpec for subsequent Set() calls.
//
// IMPORTANT: will panic if storage has been allocated via Extend.
func (inst *DataInterface) AddAttribute(a Attribute) AttributeSpec {
	return AttributeSpec{}
}

// AddAttributeToAttributeGroup adds an Attribute to a given ag
func (inst *DataInterface) AddAttributeToAttributeGroup(newAttribute Attribute, ag string) (AttributeSpec, error) {
	return AttributeSpec{}, nil
}

// GetAttribute returns an Attribute equal to the argument.
//
// TODO: Write a function to pre-compute this once we've allocated
// TODO: Write a utility function which retrieves all AttributeSpecs for
// a given instance set.
func (inst *DataInterface) GetAttribute(get Attribute) (AttributeSpec, error) {
	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	/*
for i, p := range inst.ags {
		for j, a := range p.Attributes() {
			if a.Equals(get) {
				return AttributeSpec{i, j, a}, nil
			}
		}
	}
*/
	return AttributeSpec{-1, 0, nil}, fmt.Errorf("Couldn't resolve %s", get)
}

// AllAttributes returns a slice of all Attributes.
func (inst *DataInterface) AllAttributes() []Attribute {
	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	ret := make([]Attribute, 0)
	/*for _, p := range inst.ags {
		for _, a := range p.Attributes() {
			ret = append(ret, a)
		}
	}
*/
	return ret
}

// AddClassAttribute sets an Attribute to be a class Attribute.
func (inst *DataInterface) AddClassAttribute(a Attribute) error {

	as, err := inst.GetAttribute(a)
	if err != nil {
		return err
	}

	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	inst.classAttrs[as] = true
	return nil
}

// RemoveClassAttribute removes an Attribute from the set of class Attributes.
func (inst *DataInterface) RemoveClassAttribute(a Attribute) error {
	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	as, err := inst.GetAttribute(a)
	if err != nil {
		return err
	}

	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	inst.classAttrs[as] = false
	return nil
}

// AllClassAttributes returns a slice of Attributes which have
// been designated class Attributes.
func (inst *DataInterface) AllClassAttributes() []Attribute {
	//inst.lock.Lock()
	//defer inst.lock.Unlock()
	return nil//inst.allClassAttributes()
}


//
// Allocation functions
//


// Extend extends this set of Instances to store rows additional rows.
// It's recommended to set rows to something quite large.
//
// IMPORTANT: panics if the allocation fails
func (inst *DataInterface) Extend(rows int) error {

	//inst.lock.Lock()
	//defer inst.lock.Unlock()

	if !inst.fixed {
		//err := inst.realiseAttributeGroups()
		//if err != nil {
		return nil
		//}
	}

	/*
for _, p := range inst.ags {

		// Compute ag row storage requirements
		rowSize := p.RowSizeInBytes()

		// How many bytes?
		allocSize := rows * rowSize

		p.resize(allocSize)

	}
*/
	inst.fixed = true
	inst.maxRow += rows
	return nil
}

// Set sets a particular Attribute (given as an AttributeSpec) on a particular
// row to a particular value.
//
// AttributeSpecs can be obtained using GetAttribute() or AddAttribute().
//
// IMPORTANT: Will panic() if the AttributeSpec isn't valid
//
// IMPORTANT: Will panic() if the row is too large
//
// IMPORTANT: Will panic() if the val is not the right length
func (inst *DataInterface) Set(a AttributeSpec, row int, val []byte) {
	//inst.ags[a.pond].set(a.position, row, val)
}

// Get gets a particular Attribute (given as an AttributeSpec) on a particular
// row.
// AttributeSpecs can be obtained using GetAttribute() or AddAttribute()
func (inst *DataInterface) Get(a AttributeSpec, row int) []byte {
	//return inst.ags[a.pond].get(a.position, row)
	return nil
}

// RowString returns a string representation of a given row.
func (inst *DataInterface) RowString(row int) string {
	return "TODO"
}

// MapOverRows passes each row map into a function.
// First argument is a list of AttributeSpec in the order
// they're needed in for the function. The second is the function
// to call on each row.
func (inst *DataInterface) MapOverRows(asv []AttributeSpec, mapFunc func([][]byte, int) (bool, error)) error {
	return nil
}

// Size returns the number of Attributes as the first return value
// and the maximum allocated row as the second value.
func (inst *DataInterface) Size() (int, int) {
	return len(inst.AllAttributes()), inst.maxRow
}


// String returns a human-readable summary of this dataset.
func (inst *DataInterface) String() string {
	var buffer bytes.Buffer

	// Get all Attribute information
	as := ResolveAllAttributes(inst)

	// Print header
	cols, rows := inst.Size()
	buffer.WriteString("Instances with ")
	buffer.WriteString(fmt.Sprintf("%d row(s) ", rows))
	buffer.WriteString(fmt.Sprintf("%d attribute(s)\n", cols))
	buffer.WriteString(fmt.Sprintf("Attributes: \n"))

	for _, a := range as {
		prefix := "\t"
		if inst.classAttrs[a] {
			prefix = "*\t"
		}
		buffer.WriteString(fmt.Sprintf("%s%s\n", prefix, a.attr))
	}

	buffer.WriteString("\nData:\n")
	maxRows := 30
	if rows < maxRows {
		maxRows = rows
	}

	for i := 0; i < maxRows; i++ {
		buffer.WriteString("\t")
		for _, a := range as {
			val := inst.Get(a, i)
			buffer.WriteString(fmt.Sprintf("%s ", a.attr.GetStringFromSysVal(val)))
		}
		buffer.WriteString("\n")
	}

	missingRows := rows - maxRows
	if missingRows != 0 {
		buffer.WriteString(fmt.Sprintf("\t...\n%d row(s) undisplayed", missingRows))
	} else {
		buffer.WriteString("All rows displayed")
	}

	return buffer.String()
}
