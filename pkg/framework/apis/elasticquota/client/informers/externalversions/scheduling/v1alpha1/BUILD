load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "elasticquota.go",
        "interface.go",
        "podgroup.go",
    ],
    importpath = "k8s.io/kubernetes/pkg/scheduler/framework/apis/client/informers/externalversions/scheduling/v1alpha1",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/scheduler/framework/apis/client/clientset/versioned:go_default_library",
        "//pkg/scheduler/framework/apis/client/informers/externalversions/internalinterfaces:go_default_library",
        "//pkg/scheduler/framework/apis/client/listers/scheduling/v1alpha1:go_default_library",
        "//pkg/scheduler/framework/apis/scheduling/v1alpha1:go_default_library",
        "//staging/src/k8s.io/apimachinery/pkg/apis/meta/v1:go_default_library",
        "//staging/src/k8s.io/apimachinery/pkg/runtime:go_default_library",
        "//staging/src/k8s.io/apimachinery/pkg/watch:go_default_library",
        "//staging/src/k8s.io/client-go/tools/cache:go_default_library",
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
    srcs = [":package-srcs"],
    tags = ["automanaged"],
    visibility = ["//visibility:public"],
)
