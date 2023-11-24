package util

func RemoveElements[T comparable](slice []T, indicesToRemove []int) []T {
	// 对索引进行逆序排序，以确保正确的移除
	// 避免由于移除元素导致索引发生变化
	for i := len(indicesToRemove)/2 - 1; i >= 0; i-- {
		opp := len(indicesToRemove) - 1 - i
		indicesToRemove[i], indicesToRemove[opp] = indicesToRemove[opp], indicesToRemove[i]
	}

	// 遍历要移除的索引，依次移除元素
	for _, index := range indicesToRemove {
		if index >= 0 && index < len(slice) {
			slice = append(slice[:index], slice[index+1:]...)
		}
	}

	return slice
}

func RemoveElement[T comparable](slice []T, item T) []T {

	for index, v := range slice {
		if v == item {
			slice = append(slice[:index], slice[index+1:]...)
			break
		}
	}

	return slice
}
