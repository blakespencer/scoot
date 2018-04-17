// Automatically generated by MockGen. DO NOT EDIT!
// Source: google.golang.org/genproto/googleapis/longrunning (interfaces: OperationsClient)

package mock_longrunning

import (
	gomock "github.com/golang/mock/gomock"
	empty "github.com/golang/protobuf/ptypes/empty"
	context "golang.org/x/net/context"
	longrunning "google.golang.org/genproto/googleapis/longrunning"
	grpc "google.golang.org/grpc"
)

// Mock of OperationsClient interface
type MockOperationsClient struct {
	ctrl     *gomock.Controller
	recorder *_MockOperationsClientRecorder
}

// Recorder for MockOperationsClient (not exported)
type _MockOperationsClientRecorder struct {
	mock *MockOperationsClient
}

func NewMockOperationsClient(ctrl *gomock.Controller) *MockOperationsClient {
	mock := &MockOperationsClient{ctrl: ctrl}
	mock.recorder = &_MockOperationsClientRecorder{mock}
	return mock
}

func (_m *MockOperationsClient) EXPECT() *_MockOperationsClientRecorder {
	return _m.recorder
}

func (_m *MockOperationsClient) CancelOperation(_param0 context.Context, _param1 *longrunning.CancelOperationRequest, _param2 ...grpc.CallOption) (*empty.Empty, error) {
	_s := []interface{}{_param0, _param1}
	for _, _x := range _param2 {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "CancelOperation", _s...)
	ret0, _ := ret[0].(*empty.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockOperationsClientRecorder) CancelOperation(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CancelOperation", _s...)
}

func (_m *MockOperationsClient) DeleteOperation(_param0 context.Context, _param1 *longrunning.DeleteOperationRequest, _param2 ...grpc.CallOption) (*empty.Empty, error) {
	_s := []interface{}{_param0, _param1}
	for _, _x := range _param2 {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "DeleteOperation", _s...)
	ret0, _ := ret[0].(*empty.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockOperationsClientRecorder) DeleteOperation(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DeleteOperation", _s...)
}

func (_m *MockOperationsClient) GetOperation(_param0 context.Context, _param1 *longrunning.GetOperationRequest, _param2 ...grpc.CallOption) (*longrunning.Operation, error) {
	_s := []interface{}{_param0, _param1}
	for _, _x := range _param2 {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "GetOperation", _s...)
	ret0, _ := ret[0].(*longrunning.Operation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockOperationsClientRecorder) GetOperation(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetOperation", _s...)
}

func (_m *MockOperationsClient) ListOperations(_param0 context.Context, _param1 *longrunning.ListOperationsRequest, _param2 ...grpc.CallOption) (*longrunning.ListOperationsResponse, error) {
	_s := []interface{}{_param0, _param1}
	for _, _x := range _param2 {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "ListOperations", _s...)
	ret0, _ := ret[0].(*longrunning.ListOperationsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockOperationsClientRecorder) ListOperations(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ListOperations", _s...)
}