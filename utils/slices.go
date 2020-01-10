package utils

func CompareSlices(old, new []string) (added []string, deleted []string) {
	newMap := make(map[string]struct{}, len(new))
	for _, x := range new {
		newMap[x] = struct{}{}
	}
	oldMap := make(map[string]struct{}, len(old))
	for _, x := range old {
		oldMap[x] = struct{}{}
	}
	// detect removed
	for _, x := range old {
		if _, found := newMap[x]; !found {
			deleted = append(deleted, x)
		}
	}
	// detect new
	for _, x := range new {
		if _, found := oldMap[x]; !found {
			added = append(added, x)
		}
	}
	return added, deleted
}

// MakeUnique modifies original slice
func MakeUnique(strings []string) []string {
	uniqueMap := make(map[string]struct{}, len(strings))
	uniqueSlice := strings[:0]
	for _, id := range strings {
		if _, exists := uniqueMap[id]; !exists {
			uniqueMap[id] = struct{}{}
			uniqueSlice = append(uniqueSlice, id)
		}
	}
	return uniqueSlice
}
