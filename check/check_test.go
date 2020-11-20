package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func NullQueryItem() *QueryItem {
	return &QueryItem{
		ValString: "",
		Null:      true,
	}
}

func NotNullQueryItem(s string) *QueryItem {
	return &QueryItem{
		ValString: s,
		Null:      false,
	}
}

func TestSameResults(t *testing.T) {
	result1 := [][]*QueryItem{
		{NullQueryItem(), NullQueryItem(), NullQueryItem()},
		{NullQueryItem(), NullQueryItem(), NullQueryItem()},
		{NullQueryItem(), NullQueryItem(), NotNullQueryItem("v")},
		{NullQueryItem(), NullQueryItem(), NotNullQueryItem("v")},
		{NullQueryItem(), NotNullQueryItem("v"), NotNullQueryItem("v")},
		{NotNullQueryItem("v"), NotNullQueryItem("v"), NotNullQueryItem("v")},
	}
	result2 := [][]*QueryItem{
		{NotNullQueryItem("v"), NotNullQueryItem("v"), NotNullQueryItem("v")},
		{NullQueryItem(), NotNullQueryItem("v"), NotNullQueryItem("v")},
		{NullQueryItem(), NullQueryItem(), NotNullQueryItem("v")},
		{NullQueryItem(), NullQueryItem(), NotNullQueryItem("v")},
		{NullQueryItem(), NullQueryItem(), NullQueryItem()},
		{NullQueryItem(), NullQueryItem(), NullQueryItem()},
	}
	require.Nil(t, rowsSame(result1, result2))
	require.NotNil(t, rowsSame(result1, append(result2[1:], []*QueryItem{NullQueryItem(), NullQueryItem(), NullQueryItem()})))
	require.NotNil(t, rowsSame(result1, append(result2[1:], []*QueryItem{NullQueryItem(), NullQueryItem(), NotNullQueryItem("v")})))
	require.NotNil(t, rowsSame(result1, append(result2[:6], []*QueryItem{NotNullQueryItem("v"), NotNullQueryItem("v"), NotNullQueryItem("v")})))
}
