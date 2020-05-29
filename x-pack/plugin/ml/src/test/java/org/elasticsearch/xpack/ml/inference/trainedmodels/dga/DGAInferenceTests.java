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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels.dga;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class DGAInferenceTests extends ESTestCase {

    public void testInference() throws Exception {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<TrainedModelConfig> future = new PlainActionFuture<>();
        // Should be OK as we don't make any client calls
        trainedModelProvider.getTrainedModel("dga_big_model", true, future);
        TrainedModelConfig config = future.actionGet();
        config.ensureParsedDefinition(xContentRegistry());
        TrainedModelDefinition trainedModelDefinition = config.getModelDefinition();
        Ensemble treeEnsemble = (Ensemble)trainedModelDefinition.getTrainedModel();
        List<Tree> trees = treeEnsemble.getModels().stream().map(m -> (Tree)m).collect(Collectors.toList());
        ClassificationConfig classificationConfig = new ClassificationConfig(0);
        for (int i = 0; i < 1000000; ++i) {
            List<Map<String, Object>> inferenceData = getInferenceObjects();
            StringBuilder sb = new StringBuilder();
            for (var datum : inferenceData) {
                sb.append(trainedModelDefinition.infer(datum, classificationConfig).toString());
            }
            assertThat(sb.toString(), is(not(emptyString())));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
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
