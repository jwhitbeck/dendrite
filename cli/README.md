# Dendrite cli tool

A command-line tool for interacting with dendrite files.

The recommended way of using this CLI tool is to compile it to a native binary using [GraalVM][].

    # First compile the uberjar
    lein uberjar

    # Then compile the uberjar into a native binary using GraalVM
    # Assumes that GraalVM's /bin directory is on your $PATH
    native-image --report-unsupported-elements-at-runtime \
                 --jar target/dendrite.cli-0.1.0-SNAPSHOT-standalone.jar \
                 den

    # Run the dendrite cli
    ./den --help
