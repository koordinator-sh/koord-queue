load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "clientset.go",
        "doc.go",
    ],
    importpath = "k8s.io/kubernetes/pkg/scheduler/framework/apis/client/clientset/versioned",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/scheduler/framework/apis/client/clientset/versioned/typed/scheduling/v1alpha1:go_default_library",
        "//pkg/scheduler/framework/apis/client/clientset/versioned/typed/scheduling/v1beta1:go_default_library",
        "//staging/src/k8s.io/client-go/discovery:go_default_library",
        "//staging/src/k8s.io/client-go/rest:go_default_library",
        "//staging/src/k8s.io/client-go/util/flowcontrol:go_default_library",
    ],
)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [
        ":package-srcs",
        "//pkg/scheduler/framework/apis/client/clientset/versioned/fake:all-srcs",
        "//pkg/scheduler/framework/apis/client/clientset/versioned/scheme:all-srcs",
        "//pkg/scheduler/framework/apis/client/clientset/versioned/typed/scheduling/v1alpha1:all-srcs",
        "//pkg/scheduler/framework/apis/client/clientset/versioned/typed/scheduling/v1beta1:all-srcs",
    ],
    tags = ["automanaged"],
    visibility = ["//visibility:public"],
)
