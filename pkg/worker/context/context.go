package context

// Pair represents the Key Value pairs generated
// by the Map function and sent as parameters to
// user-defined Reduce function.
type Pair struct {
	Key   string
	Value interface{}
}

// KeyValues represents a set of values with the
// same Key, and are typically passed as parameters
// to user-defined Reduce functions for processing.
type KeyValues struct {
	Key    string
	Values []interface{}
}

// Context holds the context needed to
// execute a user-defined Map function
// or Reduce function.
type Context interface {
	// ObjectName returns the name of the
	// object to be processed in the current
	// Context.
	ObjectName() string

	// Content returns the bytes of object
	// in current Context.
	Content() []byte

	// Pairs returns the all Key Value pairs
	// generated by the user-defined Map function.
	Pairs() []*Pair

	// Emit the Key Value pairs generated by the
	// user-defined Map function.
	//
	// A memory buffer will be maintained inside
	// the Context and the Key Value pairs will
	// be put into memory buffer first.
	//
	// If the buffer overflowed, the data in the
	// buffer will be written into the intermediate file
	// end emptied the buffer, and then the new Key Value
	// pairs will be put into the memory buffer.
	Emit(pair *Pair) error

	// KeyValues returns the KeyValues storage in current
	// Context.
	KeyValues() *KeyValues

	Release() error
}
