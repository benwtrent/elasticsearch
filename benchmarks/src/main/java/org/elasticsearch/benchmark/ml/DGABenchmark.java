/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.ml;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xpack.core.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;


@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class DGABenchmark {

    private static final String MODEL_RESOURCE_PATH = "/org/elasticsearch/xpack/ml/inference/persistence/";
    private static final String MODEL_RESOURCE_FILE_EXT = ".json";
    private final static List<Map<String, Object>> INFERENCE_OBJECTS = getInferenceObjects();

    private static InferenceDefinition modelDefinition;
    private static List<Map<String, Object>> preprocessedData;
    private static List<Map<String, Object>> inferenceData;
    private static InferenceConfig inferenceConfig;
    private static ExecutorService executor;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        modelDefinition = loadModelFromResource("dga_big_model");
        inferenceData = getInferenceObjects();
        new ScalingExecutorBuilder(UTILITY_THREAD_POOL_NAME,
            1, 1_000, TimeValue.timeValueMinutes(10), "xpack.ml.utility_thread_pool");
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName("test", "doop"));
        executor = EsExecutors.newScaling(
            "doop",
            1,
            100,
            TimeValue.timeValueMinutes(10).millis(),
            TimeUnit.MILLISECONDS,
            threadFactory,
            new ThreadContext(Settings.EMPTY));
        inferenceConfig = ClassificationConfig.EMPTY_PARAMS; //(0, DEFAULT_RESULTS_FIELD, DEFAULT_TOP_CLASSES_RESULTS_FIELD, null, null, numThreads);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        executor.shutdown();
    }

    /*@Benchmark
    public void infer_no_preprocessing(Blackhole bh) {
        for (var datum : preprocessedData) {
            bh.consume(modelDefinition.getTrainedModel().infer(datum, inferenceConfig, Collections.emptyMap()));
        }
    }

    @Benchmark
    public void preprocessing(Blackhole bh) {
        for (var datum : inferenceData) {
            modelDefinition.getPreProcessors().forEach(p -> p.process(datum));
        }
        bh.consume(inferenceData);
    }*/

    /*
    @Benchmark
    public void inferAndPreProcess(Blackhole bh) {
        for (var datum : inferenceData) {
            bh.consume(modelDefinition.infer(datum, inferenceConfig, executor));
        }
    }

    @Benchmark
    public void inferAndPreProcessNoThreads(Blackhole bh) {
        for (var datum : inferenceData) {
            bh.consume(modelDefinition.infer(datum, inferenceConfig));
        }
    }

     */
    @Benchmark
    public void infer_1105142(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(0), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_182200(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(1), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1274687(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(2), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1329864(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(3), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_672407(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(4), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_424703(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(5), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_147975(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(6), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1163199(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(7), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1345914(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(8), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1286491(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(9), inferenceConfig, executor));
    }

    @Benchmark
    public void infer_1105142NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(0), inferenceConfig));
    }

    @Benchmark
    public void infer_182200NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(1), inferenceConfig));
    }

    @Benchmark
    public void infer_1274687NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(2), inferenceConfig));
    }

    @Benchmark
    public void infer_1329864NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(3), inferenceConfig));
    }

    @Benchmark
    public void infer_672407NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(4), inferenceConfig));
    }

    @Benchmark
    public void infer_424703NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(5), inferenceConfig));
    }

    @Benchmark
    public void infer_147975NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(6), inferenceConfig));
    }

    @Benchmark
    public void infer_1163199NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(7), inferenceConfig));
    }

    @Benchmark
    public void infer_1345914NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(8), inferenceConfig));
    }

    @Benchmark
    public void infer_1286491NoThreads(Blackhole bh) {
        bh.consume(modelDefinition.infer(inferenceData.get(9), inferenceConfig));
    }

    private InferenceDefinition loadModelFromResource(String modelId) throws IOException {
        URL resource = getClass().getResource(MODEL_RESOURCE_PATH + modelId + MODEL_RESOURCE_FILE_EXT);
        BytesReference bytes = Streams.readFully(getClass()
            .getResourceAsStream(MODEL_RESOURCE_PATH + modelId + MODEL_RESOURCE_FILE_EXT));
        try (XContentParser parser =
                 XContentHelper.createParser(xContentRegistry(),
                     LoggingDeprecationHandler.INSTANCE,
                     bytes,
                     XContentType.JSON)) {
            TrainedModelConfig.Builder builder = TrainedModelConfig.fromXContent(parser, true);
            TrainedModelConfig config = builder.build();
            return InferenceToXContentCompressor.inflate(config.getCompressedDefinition(),
                InferenceDefinition::fromXContent,
                xContentRegistry());
        }
    }

    private NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getInferenceObjects () {
        String docs = "" +
            "{\"hits\" : [\n" +
            "      {\n" +
            "            \"f.t4\" : \"ate\",\n" +
            "            \"f.t5\" : \"ten\",\n" +
            "            \"f.t6\" : \"ene\",\n" +
            "            \"f.t7\" : \"nex\",\n" +
            "            \"f.t8\" : \"exu\",\n" +
            "            \"f.b0\" : \"cl\",\n" +
            "            \"f.b1\" : \"li\",\n" +
            "            \"f.b2\" : \"im\",\n" +
            "            \"f.b3\" : \"ma\",\n" +
            "            \"f.b4\" : \"at\",\n" +
            "            \"f.b5\" : \"te\",\n" +
            "            \"f.b6\" : \"en\",\n" +
            "            \"f.u10\" : \"u\",\n" +
            "            \"f.b7\" : \"ne\",\n" +
            "            \"f.b8\" : \"ex\",\n" +
            "            \"f.b9\" : \"xu\",\n" +
            "            \"f.u0\" : \"c\",\n" +
            "            \"f.u1\" : \"l\",\n" +
            "            \"f.u2\" : \"i\",\n" +
            "            \"f.u3\" : \"m\",\n" +
            "            \"f.u4\" : \"a\",\n" +
            "            \"f.u5\" : \"t\",\n" +
            "            \"f.u6\" : \"e\",\n" +
            "            \"f.u7\" : \"n\",\n" +
            "            \"f.u8\" : \"e\",\n" +
            "            \"f.u9\" : \"x\",\n" +
            "            \"f.tld\" : \"org\",\n" +
            "            \"f.t0\" : \"cli\",\n" +
            "            \"f.t1\" : \"lim\",\n" +
            "            \"f.t2\" : \"ima\",\n" +
            "            \"f.t3\" : \"mat\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.b10\" : \"bs\",\n" +
            "            \"f.b12\" : \"ue\",\n" +
            "            \"f.b11\" : \"su\",\n" +
            "            \"f.b14\" : \"8x\",\n" +
            "            \"f.b13\" : \"e8\",\n" +
            "            \"f.b16\" : \"zo\",\n" +
            "            \"f.b15\" : \"xz\",\n" +
            "            \"f.b18\" : \"8d\",\n" +
            "            \"f.b17\" : \"o8\",\n" +
            "            \"f.b19\" : \"do\",\n" +
            "            \"f.b0\" : \"un\",\n" +
            "            \"f.b1\" : \"ny\",\n" +
            "            \"f.b2\" : \"y8\",\n" +
            "            \"f.b3\" : \"84\",\n" +
            "            \"f.b4\" : \"4n\",\n" +
            "            \"f.t10\" : \"bsu\",\n" +
            "            \"f.b5\" : \"no\",\n" +
            "            \"f.b6\" : \"o0\",\n" +
            "            \"f.t12\" : \"ue8\",\n" +
            "            \"f.b7\" : \"0w\",\n" +
            "            \"f.t11\" : \"sue\",\n" +
            "            \"f.b8\" : \"wt\",\n" +
            "            \"f.t14\" : \"8xz\",\n" +
            "            \"f.b9\" : \"tb\",\n" +
            "            \"f.t13\" : \"e8x\",\n" +
            "            \"f.t16\" : \"zo8\",\n" +
            "            \"f.t15\" : \"xzo\",\n" +
            "            \"f.t18\" : \"8do\",\n" +
            "            \"f.t17\" : \"o8d\",\n" +
            "            \"f.t19\" : \"do6\",\n" +
            "            \"f.b21\" : \"6w\",\n" +
            "            \"f.b20\" : \"o6\",\n" +
            "            \"f.t20\" : \"o6w\",\n" +
            "            \"f.t0\" : \"uny\",\n" +
            "            \"f.t1\" : \"ny8\",\n" +
            "            \"f.t2\" : \"y84\",\n" +
            "            \"f.t3\" : \"84n\",\n" +
            "            \"f.t4\" : \"4no\",\n" +
            "            \"f.t5\" : \"no0\",\n" +
            "            \"f.t6\" : \"o0w\",\n" +
            "            \"f.t7\" : \"0wt\",\n" +
            "            \"f.t8\" : \"wtb\",\n" +
            "            \"f.t9\" : \"tbs\",\n" +
            "            \"f.u11\" : \"s\",\n" +
            "            \"f.u10\" : \"b\",\n" +
            "            \"f.u13\" : \"e\",\n" +
            "            \"f.u12\" : \"u\",\n" +
            "            \"f.u15\" : \"x\",\n" +
            "            \"f.u14\" : \"8\",\n" +
            "            \"f.u17\" : \"o\",\n" +
            "            \"f.u0\" : \"u\",\n" +
            "            \"f.u16\" : \"z\",\n" +
            "            \"f.u1\" : \"n\",\n" +
            "            \"f.u19\" : \"d\",\n" +
            "            \"f.u2\" : \"y\",\n" +
            "            \"f.u18\" : \"8\",\n" +
            "            \"f.u3\" : \"8\",\n" +
            "            \"f.u4\" : \"4\",\n" +
            "            \"f.u5\" : \"n\",\n" +
            "            \"f.u6\" : \"o\",\n" +
            "            \"f.u7\" : \"0\",\n" +
            "            \"f.u8\" : \"w\",\n" +
            "            \"f.u9\" : \"t\",\n" +
            "            \"f.tld\" : \"org\",\n" +
            "            \"f.u20\" : \"o\",\n" +
            "            \"f.u22\" : \"w\",\n" +
            "            \"f.u21\" : \"6\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.t4\" : \"acy\",\n" +
            "            \"f.t5\" : \"cys\",\n" +
            "            \"f.t6\" : \"yso\",\n" +
            "            \"f.b0\" : \"pr\",\n" +
            "            \"f.b1\" : \"ri\",\n" +
            "            \"f.b2\" : \"iv\",\n" +
            "            \"f.b3\" : \"va\",\n" +
            "            \"f.b4\" : \"ac\",\n" +
            "            \"f.b5\" : \"cy\",\n" +
            "            \"f.b6\" : \"ys\",\n" +
            "            \"f.b7\" : \"so\",\n" +
            "            \"f.u0\" : \"p\",\n" +
            "            \"f.u1\" : \"r\",\n" +
            "            \"f.u2\" : \"i\",\n" +
            "            \"f.u3\" : \"v\",\n" +
            "            \"f.u4\" : \"a\",\n" +
            "            \"f.u5\" : \"c\",\n" +
            "            \"f.u6\" : \"y\",\n" +
            "            \"f.u7\" : \"s\",\n" +
            "            \"f.u8\" : \"o\",\n" +
            "            \"f.tld\" : \"org\",\n" +
            "            \"f.t0\" : \"pri\",\n" +
            "            \"f.t1\" : \"riv\",\n" +
            "            \"f.t2\" : \"iva\",\n" +
            "            \"f.t3\" : \"vac\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.b10\" : \"al\",\n" +
            "            \"f.t4\" : \"who\",\n" +
            "            \"f.t5\" : \"hol\",\n" +
            "            \"f.t6\" : \"ole\",\n" +
            "            \"f.t7\" : \"les\",\n" +
            "            \"f.t8\" : \"esa\",\n" +
            "            \"f.t9\" : \"sal\",\n" +
            "            \"f.b0\" : \"re\",\n" +
            "            \"f.b1\" : \"em\",\n" +
            "            \"f.b2\" : \"mn\",\n" +
            "            \"f.b3\" : \"nw\",\n" +
            "            \"f.b4\" : \"wh\",\n" +
            "            \"f.b5\" : \"ho\",\n" +
            "            \"f.u11\" : \"l\",\n" +
            "            \"f.b6\" : \"ol\",\n" +
            "            \"f.u10\" : \"a\",\n" +
            "            \"f.b7\" : \"le\",\n" +
            "            \"f.b8\" : \"es\",\n" +
            "            \"f.b9\" : \"sa\",\n" +
            "            \"f.u0\" : \"r\",\n" +
            "            \"f.u1\" : \"e\",\n" +
            "            \"f.u2\" : \"m\",\n" +
            "            \"f.u3\" : \"n\",\n" +
            "            \"f.u4\" : \"w\",\n" +
            "            \"f.u5\" : \"h\",\n" +
            "            \"f.u6\" : \"o\",\n" +
            "            \"f.u7\" : \"l\",\n" +
            "            \"f.u8\" : \"e\",\n" +
            "            \"f.u9\" : \"s\",\n" +
            "            \"f.tld\" : \"com\",\n" +
            "            \"f.t0\" : \"rem\",\n" +
            "            \"f.t1\" : \"emn\",\n" +
            "            \"f.t2\" : \"mnw\",\n" +
            "            \"f.t3\" : \"nwh\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.u5\" : \"q\",\n" +
            "            \"f.t4\" : \"tqw\",\n" +
            "            \"f.u6\" : \"w\",\n" +
            "            \"f.tld\" : \"com\",\n" +
            "            \"f.b0\" : \"ou\",\n" +
            "            \"f.b1\" : \"uo\",\n" +
            "            \"f.b2\" : \"od\",\n" +
            "            \"f.b3\" : \"dt\",\n" +
            "            \"f.b4\" : \"tq\",\n" +
            "            \"f.b5\" : \"qw\",\n" +
            "            \"f.u0\" : \"o\",\n" +
            "            \"f.u1\" : \"u\",\n" +
            "            \"f.t0\" : \"ouo\",\n" +
            "            \"f.u2\" : \"o\",\n" +
            "            \"f.t1\" : \"uod\",\n" +
            "            \"f.u3\" : \"d\",\n" +
            "            \"f.t2\" : \"odt\",\n" +
            "            \"f.u4\" : \"t\",\n" +
            "            \"f.t3\" : \"dtq\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.t4\" : \"itb\",\n" +
            "            \"f.t5\" : \"tbe\",\n" +
            "            \"f.t6\" : \"bei\",\n" +
            "            \"f.b0\" : \"xe\",\n" +
            "            \"f.b1\" : \"ea\",\n" +
            "            \"f.b2\" : \"aq\",\n" +
            "            \"f.b3\" : \"qi\",\n" +
            "            \"f.b4\" : \"it\",\n" +
            "            \"f.b5\" : \"tb\",\n" +
            "            \"f.b6\" : \"be\",\n" +
            "            \"f.b7\" : \"ei\",\n" +
            "            \"f.u0\" : \"x\",\n" +
            "            \"f.u1\" : \"e\",\n" +
            "            \"f.u2\" : \"a\",\n" +
            "            \"f.u3\" : \"q\",\n" +
            "            \"f.u4\" : \"i\",\n" +
            "            \"f.u5\" : \"t\",\n" +
            "            \"f.u6\" : \"b\",\n" +
            "            \"f.u7\" : \"e\",\n" +
            "            \"f.u8\" : \"i\",\n" +
            "            \"f.tld\" : \"kz\",\n" +
            "            \"f.t0\" : \"xea\",\n" +
            "            \"f.t1\" : \"eaq\",\n" +
            "            \"f.t2\" : \"aqi\",\n" +
            "            \"f.t3\" : \"qit\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.b10\" : \"b6\",\n" +
            "            \"f.b12\" : \"46\",\n" +
            "            \"f.b11\" : \"64\",\n" +
            "            \"f.b14\" : \"36\",\n" +
            "            \"f.b13\" : \"63\",\n" +
            "            \"f.b16\" : \"d2\",\n" +
            "            \"f.b15\" : \"6d\",\n" +
            "            \"f.b18\" : \"54\",\n" +
            "            \"f.b17\" : \"25\",\n" +
            "            \"f.b19\" : \"47\",\n" +
            "            \"f.b0\" : \"yf\",\n" +
            "            \"f.b1\" : \"fe\",\n" +
            "            \"f.b2\" : \"e7\",\n" +
            "            \"f.b3\" : \"72\",\n" +
            "            \"f.b4\" : \"28\",\n" +
            "            \"f.b5\" : \"88\",\n" +
            "            \"f.b6\" : \"89\",\n" +
            "            \"f.b7\" : \"95\",\n" +
            "            \"f.b8\" : \"50\",\n" +
            "            \"f.b9\" : \"0b\",\n" +
            "            \"f.b21\" : \"6b\",\n" +
            "            \"f.b20\" : \"76\",\n" +
            "            \"f.b23\" : \"35\",\n" +
            "            \"f.b22\" : \"b3\",\n" +
            "            \"f.b25\" : \"3a\",\n" +
            "            \"f.b24\" : \"53\",\n" +
            "            \"f.b27\" : \"ac\",\n" +
            "            \"f.b26\" : \"aa\",\n" +
            "            \"f.b29\" : \"ba\",\n" +
            "            \"f.b28\" : \"cb\",\n" +
            "            \"f.t0\" : \"yfe\",\n" +
            "            \"f.t1\" : \"fe7\",\n" +
            "            \"f.b30\" : \"a9\",\n" +
            "            \"f.t2\" : \"e72\",\n" +
            "            \"f.t3\" : \"728\",\n" +
            "            \"f.t4\" : \"288\",\n" +
            "            \"f.b31\" : \"92\",\n" +
            "            \"f.t5\" : \"889\",\n" +
            "            \"f.t6\" : \"895\",\n" +
            "            \"f.t7\" : \"950\",\n" +
            "            \"f.t8\" : \"50b\",\n" +
            "            \"f.t9\" : \"0b6\",\n" +
            "            \"f.u11\" : \"6\",\n" +
            "            \"f.u10\" : \"b\",\n" +
            "            \"f.u13\" : \"6\",\n" +
            "            \"f.u12\" : \"4\",\n" +
            "            \"f.u15\" : \"6\",\n" +
            "            \"f.u14\" : \"3\",\n" +
            "            \"f.u17\" : \"2\",\n" +
            "            \"f.u0\" : \"y\",\n" +
            "            \"f.u16\" : \"d\",\n" +
            "            \"f.u1\" : \"f\",\n" +
            "            \"f.u19\" : \"4\",\n" +
            "            \"f.u2\" : \"e\",\n" +
            "            \"f.u18\" : \"5\",\n" +
            "            \"f.u3\" : \"7\",\n" +
            "            \"f.u4\" : \"2\",\n" +
            "            \"f.u5\" : \"8\",\n" +
            "            \"f.u6\" : \"8\",\n" +
            "            \"f.u7\" : \"9\",\n" +
            "            \"f.u8\" : \"5\",\n" +
            "            \"f.u9\" : \"0\",\n" +
            "            \"f.u20\" : \"7\",\n" +
            "            \"f.u22\" : \"b\",\n" +
            "            \"f.u21\" : \"6\",\n" +
            "            \"f.u24\" : \"5\",\n" +
            "            \"f.u23\" : \"3\",\n" +
            "            \"f.u26\" : \"a\",\n" +
            "            \"f.u25\" : \"3\",\n" +
            "            \"f.u28\" : \"c\",\n" +
            "            \"f.u27\" : \"a\",\n" +
            "            \"f.u29\" : \"b\",\n" +
            "            \"f.u31\" : \"9\",\n" +
            "            \"f.t10\" : \"b64\",\n" +
            "            \"f.u30\" : \"a\",\n" +
            "            \"f.t12\" : \"463\",\n" +
            "            \"f.u32\" : \"2\",\n" +
            "            \"f.t11\" : \"646\",\n" +
            "            \"f.t14\" : \"36d\",\n" +
            "            \"f.t13\" : \"636\",\n" +
            "            \"f.t16\" : \"d25\",\n" +
            "            \"f.t15\" : \"6d2\",\n" +
            "            \"f.t18\" : \"547\",\n" +
            "            \"f.t17\" : \"254\",\n" +
            "            \"f.t19\" : \"476\",\n" +
            "            \"f.t21\" : \"6b3\",\n" +
            "            \"f.t20\" : \"76b\",\n" +
            "            \"f.t23\" : \"353\",\n" +
            "            \"f.t22\" : \"b35\",\n" +
            "            \"f.t25\" : \"3aa\",\n" +
            "            \"f.t24\" : \"53a\",\n" +
            "            \"f.t27\" : \"acb\",\n" +
            "            \"f.t26\" : \"aac\",\n" +
            "            \"f.t29\" : \"ba9\",\n" +
            "            \"f.t28\" : \"cba\",\n" +
            "            \"f.t30\" : \"a92\",\n" +
            "            \"f.tld\" : \"cc\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.u5\" : \"s\",\n" +
            "            \"f.t4\" : \"bst\",\n" +
            "            \"f.u6\" : \"t\",\n" +
            "            \"f.t5\" : \"ste\",\n" +
            "            \"f.u7\" : \"e\",\n" +
            "            \"f.tld\" : \"com\",\n" +
            "            \"f.b0\" : \"wp\",\n" +
            "            \"f.b1\" : \"pj\",\n" +
            "            \"f.b2\" : \"jo\",\n" +
            "            \"f.b3\" : \"ob\",\n" +
            "            \"f.b4\" : \"bs\",\n" +
            "            \"f.b5\" : \"st\",\n" +
            "            \"f.b6\" : \"te\",\n" +
            "            \"f.u0\" : \"w\",\n" +
            "            \"f.u1\" : \"p\",\n" +
            "            \"f.t0\" : \"wpj\",\n" +
            "            \"f.u2\" : \"j\",\n" +
            "            \"f.t1\" : \"pjo\",\n" +
            "            \"f.u3\" : \"o\",\n" +
            "            \"f.t2\" : \"job\",\n" +
            "            \"f.u4\" : \"b\",\n" +
            "            \"f.t3\" : \"obs\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.b10\" : \"gr\",\n" +
            "            \"f.t4\" : \"nti\",\n" +
            "            \"f.t5\" : \"tim\",\n" +
            "            \"f.b12\" : \"ou\",\n" +
            "            \"f.t6\" : \"ime\",\n" +
            "            \"f.b11\" : \"ro\",\n" +
            "            \"f.t7\" : \"mes\",\n" +
            "            \"f.t8\" : \"esg\",\n" +
            "            \"f.t9\" : \"sgr\",\n" +
            "            \"f.b0\" : \"gr\",\n" +
            "            \"f.b1\" : \"re\",\n" +
            "            \"f.b2\" : \"ee\",\n" +
            "            \"f.b3\" : \"en\",\n" +
            "            \"f.b4\" : \"nt\",\n" +
            "            \"f.t10\" : \"gro\",\n" +
            "            \"f.b5\" : \"ti\",\n" +
            "            \"f.u11\" : \"r\",\n" +
            "            \"f.b6\" : \"im\",\n" +
            "            \"f.u10\" : \"g\",\n" +
            "            \"f.b7\" : \"me\",\n" +
            "            \"f.t11\" : \"rou\",\n" +
            "            \"f.u13\" : \"u\",\n" +
            "            \"f.b8\" : \"es\",\n" +
            "            \"f.u12\" : \"o\",\n" +
            "            \"f.b9\" : \"sg\",\n" +
            "            \"f.u0\" : \"g\",\n" +
            "            \"f.u1\" : \"r\",\n" +
            "            \"f.u2\" : \"e\",\n" +
            "            \"f.u3\" : \"e\",\n" +
            "            \"f.u4\" : \"n\",\n" +
            "            \"f.u5\" : \"t\",\n" +
            "            \"f.u6\" : \"i\",\n" +
            "            \"f.u7\" : \"m\",\n" +
            "            \"f.u8\" : \"e\",\n" +
            "            \"f.u9\" : \"s\",\n" +
            "            \"f.tld\" : \"com\",\n" +
            "            \"f.t0\" : \"gre\",\n" +
            "            \"f.t1\" : \"ree\",\n" +
            "            \"f.t2\" : \"een\",\n" +
            "            \"f.t3\" : \"ent\"\n" +
            "      },\n" +
            "      {\n" +
            "            \"f.u5\" : \"n\",\n" +
            "            \"f.t4\" : \"enh\",\n" +
            "            \"f.u6\" : \"h\",\n" +
            "            \"f.t5\" : \"nha\",\n" +
            "            \"f.u7\" : \"a\",\n" +
            "            \"f.tld\" : \"org\",\n" +
            "            \"f.b0\" : \"my\",\n" +
            "            \"f.b1\" : \"yo\",\n" +
            "            \"f.b2\" : \"op\",\n" +
            "            \"f.b3\" : \"pe\",\n" +
            "            \"f.b4\" : \"en\",\n" +
            "            \"f.b5\" : \"nh\",\n" +
            "            \"f.b6\" : \"ha\",\n" +
            "            \"f.u0\" : \"m\",\n" +
            "            \"f.u1\" : \"y\",\n" +
            "            \"f.t0\" : \"myo\",\n" +
            "            \"f.u2\" : \"o\",\n" +
            "            \"f.t1\" : \"yop\",\n" +
            "            \"f.u3\" : \"p\",\n" +
            "            \"f.t2\" : \"ope\",\n" +
            "            \"f.u4\" : \"e\",\n" +
            "            \"f.t3\" : \"pen\"\n" +
            "      }\n" +
            "    ]}";
        Map<String, Object> hits = XContentHelper.convertToMap(JsonXContent.jsonXContent, docs, false);
        return (List<Map<String, Object>>)hits.get("hits");
    }

}
