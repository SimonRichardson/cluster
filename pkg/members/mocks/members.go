// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/SimonRichardson/cluster/pkg/members (interfaces: Members,MemberList,Member)

package mocks

import (
	members "github.com/SimonRichardson/cluster/pkg/members"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockMembers is a mock of Members interface
type MockMembers struct {
	ctrl     *gomock.Controller
	recorder *MockMembersMockRecorder
}

// MockMembersMockRecorder is the mock recorder for MockMembers
type MockMembersMockRecorder struct {
	mock *MockMembers
}

// NewMockMembers creates a new mock instance
func NewMockMembers(ctrl *gomock.Controller) *MockMembers {
	mock := &MockMembers{ctrl: ctrl}
	mock.recorder = &MockMembersMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockMembers) EXPECT() *MockMembersMockRecorder {
	return _m.recorder
}

// Close mocks base method
func (_m *MockMembers) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (_mr *MockMembersMockRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Close", reflect.TypeOf((*MockMembers)(nil).Close))
}

// Join mocks base method
func (_m *MockMembers) Join() (int, error) {
	ret := _m.ctrl.Call(_m, "Join")
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Join indicates an expected call of Join
func (_mr *MockMembersMockRecorder) Join() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Join", reflect.TypeOf((*MockMembers)(nil).Join))
}

// Leave mocks base method
func (_m *MockMembers) Leave() error {
	ret := _m.ctrl.Call(_m, "Leave")
	ret0, _ := ret[0].(error)
	return ret0
}

// Leave indicates an expected call of Leave
func (_mr *MockMembersMockRecorder) Leave() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Leave", reflect.TypeOf((*MockMembers)(nil).Leave))
}

// MemberList mocks base method
func (_m *MockMembers) MemberList() members.MemberList {
	ret := _m.ctrl.Call(_m, "MemberList")
	ret0, _ := ret[0].(members.MemberList)
	return ret0
}

// MemberList indicates an expected call of MemberList
func (_mr *MockMembersMockRecorder) MemberList() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "MemberList", reflect.TypeOf((*MockMembers)(nil).MemberList))
}

// Walk mocks base method
func (_m *MockMembers) Walk(_param0 func(members.PeerInfo) error) error {
	ret := _m.ctrl.Call(_m, "Walk", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Walk indicates an expected call of Walk
func (_mr *MockMembersMockRecorder) Walk(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Walk", reflect.TypeOf((*MockMembers)(nil).Walk), arg0)
}

// MockMemberList is a mock of MemberList interface
type MockMemberList struct {
	ctrl     *gomock.Controller
	recorder *MockMemberListMockRecorder
}

// MockMemberListMockRecorder is the mock recorder for MockMemberList
type MockMemberListMockRecorder struct {
	mock *MockMemberList
}

// NewMockMemberList creates a new mock instance
func NewMockMemberList(ctrl *gomock.Controller) *MockMemberList {
	mock := &MockMemberList{ctrl: ctrl}
	mock.recorder = &MockMemberListMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockMemberList) EXPECT() *MockMemberListMockRecorder {
	return _m.recorder
}

// LocalNode mocks base method
func (_m *MockMemberList) LocalNode() members.Member {
	ret := _m.ctrl.Call(_m, "LocalNode")
	ret0, _ := ret[0].(members.Member)
	return ret0
}

// LocalNode indicates an expected call of LocalNode
func (_mr *MockMemberListMockRecorder) LocalNode() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LocalNode", reflect.TypeOf((*MockMemberList)(nil).LocalNode))
}

// Members mocks base method
func (_m *MockMemberList) Members() []members.Member {
	ret := _m.ctrl.Call(_m, "Members")
	ret0, _ := ret[0].([]members.Member)
	return ret0
}

// Members indicates an expected call of Members
func (_mr *MockMemberListMockRecorder) Members() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Members", reflect.TypeOf((*MockMemberList)(nil).Members))
}

// NumMembers mocks base method
func (_m *MockMemberList) NumMembers() int {
	ret := _m.ctrl.Call(_m, "NumMembers")
	ret0, _ := ret[0].(int)
	return ret0
}

// NumMembers indicates an expected call of NumMembers
func (_mr *MockMemberListMockRecorder) NumMembers() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "NumMembers", reflect.TypeOf((*MockMemberList)(nil).NumMembers))
}

// MockMember is a mock of Member interface
type MockMember struct {
	ctrl     *gomock.Controller
	recorder *MockMemberMockRecorder
}

// MockMemberMockRecorder is the mock recorder for MockMember
type MockMemberMockRecorder struct {
	mock *MockMember
}

// NewMockMember creates a new mock instance
func NewMockMember(ctrl *gomock.Controller) *MockMember {
	mock := &MockMember{ctrl: ctrl}
	mock.recorder = &MockMemberMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockMember) EXPECT() *MockMemberMockRecorder {
	return _m.recorder
}

// Name mocks base method
func (_m *MockMember) Name() string {
	ret := _m.ctrl.Call(_m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name
func (_mr *MockMemberMockRecorder) Name() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Name", reflect.TypeOf((*MockMember)(nil).Name))
}
