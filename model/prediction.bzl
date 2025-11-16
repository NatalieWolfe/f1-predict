""""""

def genprediction(name, year, circuit):
    native.genrule(
        name = "_%s_input" % name,
        srcs = ["//data:%s_%s_results" % (year, circuit)],
        outs = ["%s_input.csv" % name],
        cmd = (
            "$(location //model:generate_training_files)" +
            "  --prediction_file=$(OUTS)" +
            "  $(SRCS)"
        ),
        tools = ["//model:generate_training_files"],
    )

    native.genrule(
        name = "_%s_f1_lambdarank_output" % name,
        srcs = [
            "//model:f1_lambdarank_model.txt",
            "//model:predict.conf",
            ":%s_input.csv" % name,
        ],
        outs = ["%s_f1_lambdarank_output.txt" % name],
        cmd = (
            "$(location //third_party/lightgbm:binary)" +
            "  config=$(location //model:predict.conf)" +
            "  input_model=$(location //model:f1_lambdarank_model.txt)" +
            "  data=$(location :%s_input.csv)" % name +
            "  output_result=$(OUTS)" +
            "  2>&1" +
            "  | tail"
        ),
        tools = ["//third_party/lightgbm:binary"],
    )

    native.genrule(
        name = name,
        srcs = [
            ":%s_input.csv" % name,
            ":%s_f1_lambdarank_output.txt" % name,
            "//data:%s_%s_results" % (year, circuit),
        ],
        outs = ["%s.textproto" % name],
        cmd = (
            "$(location //model:apply_prediction)" +
            "  --source_results=$(location :%s_input.csv)" % name +
            "  --prediction_file=$(location :%s_f1_lambdarank_output.txt)" % name +
            "  --output=$(OUTS)"
        ),
        tools = ["//model:apply_prediction"],
    )
