/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.ml;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.getDenseVectorFieldType;

public class SemanticTextFieldMapper extends FieldMapper {
    protected final Logger logger = LogManager.getLogger(getClass());
    public static final String CONTENT_TYPE = "semantic_text";

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<String> model = Parameter.stringParam("model", false, m -> toType(m).fieldType().model, null);
        private final Parameter<Integer> dims = Parameter.intParam("dims", true, m -> toType(m).fieldType().dims, -1);
        private final Parameter<String> similarity = Parameter.stringParam(
            "similarity",
            true,
            m -> Optional.ofNullable(toType(m).fieldType().vectorSimilarity)
                .map(DenseVectorFieldMapper.VectorSimilarity::toString)
                .orElse(null),
            null
        );
        private final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected FieldMapper.Parameter<?>[] getParameters() {
            return new FieldMapper.Parameter<?>[] { model, dims, similarity, meta };
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            return new SemanticTextFieldMapper(
                name,
                new SemanticTextFieldType(
                    context.buildFullName(name),
                    model.getValue(),
                    similarity.getValue(),
                    dims.getValue(),
                    meta.getValue()
                )
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class SemanticTextFieldType extends MappedFieldType {

        private final String model;
        private final DenseVectorFieldMapper.VectorSimilarity vectorSimilarity;
        private final int dims;

        public SemanticTextFieldType(String name, String model, String vectorSimilarity, int dims, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.model = model;
            this.vectorSimilarity = vectorSimilarity == null ? null : DenseVectorFieldMapper.VectorSimilarity.fromString(vectorSimilarity);
            this.dims = dims;
        }

        public String model() {
            return model;
        }

        public DenseVectorFieldMapper.VectorSimilarity vectorSimilarity() {
            return vectorSimilarity;
        }

        public int dims() {
            return dims;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return TextFieldMapper.CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        @Override
        public Query createKnnQuery(float[] queryVector, int numCands, Query filter, Float similarityThreshold) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }

            if (queryVector.length != dims) {
                throw new IllegalArgumentException(
                    "the query vector has a different dimension [" + queryVector.length + "] than the index vectors [" + dims + "]"
                );
            }

            return new KnnFloatVectorQuery(name(), queryVector, numCands, filter);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new VectorIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.FLOAT,
                dims,
                true
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

    }

    SemanticTextFieldMapper(String simpleName, MappedFieldType mappedFieldType) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (context.sourceToParse().additionalInfo != null && context.sourceToParse().additionalInfo.isEmpty() == false) {
            float[] info = (float[]) context.sourceToParse().additionalInfo.get(name());
            logger.info("has additional info!!!");
            if (info != null) {
                if (fieldType().dims < 0) {
                    Map<String, String> newMeta = new HashMap<>(fieldType().meta());
                    newMeta.put("model", fieldType().model());
                    newMeta.put("dims", Integer.toString(info.length));
                    SemanticTextFieldType newFieldType = new SemanticTextFieldType(
                        fieldType().name(),
                        fieldType().model,
                        DenseVectorFieldMapper.VectorSimilarity.COSINE.toString(),
                        info.length,
                        newMeta
                    );
                    Mapper update = new SemanticTextFieldMapper(simpleName(), newFieldType);
                    context.addDynamicMapper(update);
                    logger.info("Asking for mapping update");
                    return;
                }
                if (fieldType().dims != info.length) {
                    throw new IllegalArgumentException();
                }
                FieldType denseVectorFieldType = getDenseVectorFieldType(
                    info.length,
                    VectorEncoding.FLOAT32,
                    DenseVectorFieldMapper.VectorSimilarity.COSINE.function
                );
                context.doc().addWithKey(fieldType().name(), new KnnFloatVectorField(simpleName(), info, denseVectorFieldType));
                logger.info("Indexed the dense vector");
                return;
            }
        }
        String inputText = context.parser().text();
        logger.info("Asking for inference");
        context.asyncActions.add((c, l) -> {
            c.execute(
                InferModelAction.INSTANCE,
                InferModelAction.Request.forTextInput(fieldType().model, new EmptyConfigUpdate(), List.of(inputText)),
                ActionListener.wrap(ir -> {
                    logger.info("Inference response occurred");
                    if (ir.getInferenceResults().isEmpty()) {
                        throw new IllegalArgumentException();
                    }
                    if (ir.getInferenceResults().get(0) instanceof TextEmbeddingResults ter) {
                        l.onResponse(Tuple.tuple(name(), ter.getInferenceAsFloat()));
                        return;
                    }
                    throw new IllegalArgumentException();
                }, e -> {
                    logger.error("Inference failure occurred", e);
                    l.onFailure(e);
                })
            );
        });
    }
}
