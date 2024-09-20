package co.elastic.michael.bischoff.fixilm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.ilm.MoveToStepRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ModifyDataStreamRequest;
import co.elastic.clients.elasticsearch.indices.ModifyDataStreamResponse;
import co.elastic.clients.elasticsearch.searchable_snapshots.MountRequest;
import co.elastic.clients.elasticsearch.searchable_snapshots.MountResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class FixILM implements Callable<Integer> {

    private final static Logger logger = LoggerFactory.getLogger(FixILM.class);
    @CommandLine.Option(names = { "-v", "--verbose" },
            description = """
                Verbose mode. Helpful for troubleshooting.
                Multiple -v options increase the verbosity.
                """)
    private boolean[] verbose = new boolean[0];
    @CommandLine.Option(names = {"--cloudid"}, description = "elastic Cloud ID", required = true)
    private String cloudId;
    @CommandLine.Option(names = {"--apiKey"}, description = "elastic cloud apiKey", required = true)
    private String apiKey;
    @CommandLine.Option(names = {"--oldRepo"}, description = "the old repository (_clone_<hash>", required = true)
    private String oldRepo;
    @CommandLine.Option(names = {"--limit"}, defaultValue = "-1", description = "the maximum number of command to generate>")
    private int limit;
    @CommandLine.Option(names = {"--rendezvousAction"}, defaultValue = "false", negatable = true, description = "apply the rendezvous actions(default: ${DEFAULT-VALUE})")
    private boolean performRendezvousAction;


    public static void main(String[] args) {
        int exitCode = new CommandLine(new FixILM()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws RuntimeException, IOException {
        updateLogging(verbose);

        try (RestClient restClient = getClient(cloudId,apiKey)) {
            ElasticsearchClient client = createESClient(restClient);
            Map<String, String> datastreamLookup = collectDataStreamInfo(restClient);
            handleIndicesWithILMIssues(restClient, client, datastreamLookup);
        }

        return 0;
    }

    private Map<String, String> collectDataStreamInfo(RestClient client) throws IOException {
        Request request = new Request("GET", "/_data_stream");
        request.addParameter("filter_path", "data_streams.name,**.index_name");
        Response response = client.performRequest(request);

        JsonArray dataStreams = (JsonArray) JsonData.from(response.getEntity().getContent()).to(Map.class).get("data_streams");
        Map<String, String> datastreamLookup = new HashMap<>();
        dataStreams.stream().map(JsonValue::asJsonObject).forEach(entry -> {
            String dataStreamName = entry.getString("name");
            entry.getJsonArray("indices").forEach(index -> {
              datastreamLookup.put(index.asJsonObject().getString("index_name"), dataStreamName);
            });
        });
        return datastreamLookup;
    }

    private String originalIndexName(RestClient client, String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("filter_path", "**.store.snapshot");
        Response response = client.performRequest(request);

        JsonObject snapshot = JsonData.from(response.getEntity().getContent()).to(JsonObject.class).getJsonObject(index).getJsonObject("settings").getJsonObject("index").getJsonObject("store").getJsonObject("snapshot");

        return snapshot.getString("index_name");
    }

    private static ElasticsearchClient createESClient(RestClient restClient) {
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private RestClient getClient(String cloudId, String apiKey) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(60000)
                .build();
        RequestOptions options = RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(requestConfig)
                .build();

        return RestClient
                .builder(cloudId)
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + apiKey)
                })
                .setRequestConfigCallback(builder -> builder.setConnectTimeout(5000).setSocketTimeout(60000))
                .build();
    }

    private void handleIndicesWithILMIssues(RestClient client, ElasticsearchClient elasticsearchClient, Map<String, String> datastreamLookup) throws IOException {
        Collection<IndexToFix> indicesToFix = new ArrayList<>();
        Request request = new Request("GET", "/_all/_ilm/explain");
        request.addParameter("only_errors", "true");

        Response response = client.performRequest(request);

        Map<String, Object> responseMap = JsonData.from(response.getEntity().getContent()).to(Map.class);

        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        for (Map.Entry<String, Object> entry : indices.entrySet()) {
            String indexName = entry.getKey();
            Map<String, Object> ilmInfo = (Map<String, Object>) entry.getValue();

            String phase = ((JsonString) ilmInfo.get("phase")).getString();
            String action = ((JsonString) ilmInfo.get("action")).getString();
            String step = ((JsonString) ilmInfo.get("step")).getString();
            JsonString snapshotNameString = ((JsonString) ilmInfo.get("snapshot_name"));
            if(snapshotNameString==null) {
                if("cold".equals(phase) && "searchable_snapshot".equals(action) && "ERROR".equals(step) && indexName.startsWith("fixed-")) {
                    if(performRendezvousAction) rendezvousILMState(elasticsearchClient, indexName);
                }
                continue;
            }
            String snapshotName = snapshotNameString.getString();
            String ilmPolicy = ((JsonString) ilmInfo.get("policy")).getString();

            Map<String, JsonString> stepInfo = (Map<String, JsonString>) ilmInfo.get("step_info");
            String stepInfoType = stepInfo.get("type").getString();
            String stepInfoReason = stepInfo.get("reason").getString();

            if("frozen".equals(phase) && "searchable_snapshot".equals(action) && "ERROR".equals(step) && ("index_not_found_exception".equals(stepInfoType) && stepInfoReason.endsWith("not found in repository [found-snapshots]]") || "exception".equals(stepInfoType) && stepInfoReason.endsWith("not found in repository [found-snapshots]"))) {
                String datastream = datastreamLookup.get(indexName);
                String originalIndexName = originalIndexName(client, indexName);
                IndexToFix index = new IndexToFix(indexName, originalIndexName, phase, action, step, snapshotName, ilmPolicy, datastream);
                indicesToFix.add(index);

                if(!mountPartialSnapshot(elasticsearchClient, index))    throw new RuntimeException("Could not mount partial snapshot for index (" + index + ")");
                if(!moveToFrozenComplete(elasticsearchClient, index))    throw new RuntimeException("Could not move to frozen phase, complete step for index (" + index + ")");
                if(!updateDataStream(elasticsearchClient, index))        throw new RuntimeException("Could not update datastream for index (" + index + ")");
                if(!deleteOriginalIndex(elasticsearchClient, index))     throw new RuntimeException("Could not delete original index (" + index + ")");

                logger.info("Finished with index (" + index + ")");

                if(limit != -1 && indicesToFix.size() >= limit) break;
            }

            if("cold".equals(phase) && "searchable_snapshot".equals(action) && "ERROR".equals(step) && indexName.startsWith("fixed-")) {
                if(performRendezvousAction) rendezvousILMState(elasticsearchClient, indexName);
            }
        }
    }

    private void rendezvousILMState(ElasticsearchClient client, String indexName) {
        MoveToStepRequest request = new MoveToStepRequest.Builder()
                .index(indexName)
                .currentStep(b -> b.phase("cold").action("searchable_snapshot").name("ERROR"))
                .nextStep(b -> b.phase("frozen").action("complete").name("complete"))
                .build();
        try {
            client.ilm().moveToStep(request);
            logger.debug("performed rendezvous action for " + indexName);
        } catch (IOException e) {
            logger.warn("RendezvousMove for index " + indexName + " failed",e);
        }
    }

    private boolean updateDataStream(ElasticsearchClient client, IndexToFix index) throws IOException {
        String updateDatastreamRequest = "POST _data_stream/_modify\n" +
                "{\n" +
                "    \"actions\": [\n" +
                "        {\n" +
                "            \"remove_backing_index\": {\n" +
                "                \"data_stream\": \"" + index.datastream + "\",\n" +
                "                \"index\": \"" + index.indexName + "\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"add_backing_index\": {\n" +
                "                \"data_stream\": \"" + index.datastream + "\",\n" +
                "                \"index\": \"fixed-partial-" + index.indexName + "\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";
        logger.info(updateDatastreamRequest);

        ModifyDataStreamRequest request = new ModifyDataStreamRequest.Builder()
                .actions(a -> a.removeBackingIndex(b -> b.dataStream(index.datastream).index(index.indexName)))
                .actions(a -> a.addBackingIndex(b -> b.dataStream(index.datastream).index("fixed-partial-" + index.indexName)))
                .build();

        ModifyDataStreamResponse response = client.indices().modifyDataStream(request);
        return response.acknowledged();
    }

    private boolean deleteOriginalIndex(ElasticsearchClient client, IndexToFix index) throws IOException {
        String deleteOldIndexRequest = "DELETE " + index.indexName + "\n";
        logger.info(deleteOldIndexRequest);

        DeleteIndexRequest request = new DeleteIndexRequest.Builder()
                .index(index.indexName)
                .build();
        DeleteIndexResponse response = client.indices().delete(request);
        logger.debug("Response: " + response);

        if(response.acknowledged()) return true;

        // Try again not sure why it won't acknowledge
        try {
            DeleteIndexResponse response2 = client.indices().delete(request);
            return response2.acknowledged();
        } catch (ElasticsearchException e) {
            //if it doesn't exist then it was deleted
            if("index_not_found_exception".equals(e.error().type())) {
                return true;
            }
            throw e;
        }
    }

    private boolean moveToFrozenComplete(ElasticsearchClient client, IndexToFix index) throws IOException {
        String moveToFrozenCompletedRequest = "POST /_ilm/move/fixed-partial-" + index.indexName + "\n" +
                "{\n" +
                "    \"current_step\": {\n" +
                "        \"phase\": \"cold\",\n" +
                "        \"action\": \"searchable_snapshot\",\n" +
                "        \"name\": \"wait-for-shard-history-leases\"\n" +
                "    },\n" +
                "    \"next_step\": {\n" +
                "        \"phase\": \"frozen\",\n" +
                "        \"action\": \"complete\"\n" +
                "    }\n" +
                "}\n";
        logger.info(moveToFrozenCompletedRequest);

        MoveToStepRequest request = new MoveToStepRequest.Builder()
                .index("fixed-partial-" + index.indexName)
                .currentStep(b -> b.phase("cold").action("searchable_snapshot").name("wait-for-shard-history-leases"))
                .nextStep(b -> b.phase("frozen").action("complete").name("complete"))
                .build();
        try {
            client.ilm().moveToStep(request);
            return true;
        } catch (ElasticsearchException e) {
            //best effort else we pick this up later manually or automatically if it was a race condition as it will end up in the error step.
            String expectedline = "index [fixed-partial-" + index.indexName + "] is not on current step [{\"phase\":\"cold\",\"action\":\"searchable_snapshot\",\"name\":\"wait-for-shard-history-leases\"}], currently: [{";
            if(e.status() == 400 && e.response().error().reason().startsWith(expectedline)) {
                //try again with curren action / step
                String[] current = e.response().error().reason().substring(expectedline.length(),e.response().error().reason().length()-2).split(",");

                if(!current[0].startsWith("\"phase\":\"")) throw e;
                String phase = current[0].substring("\"phase\":\"".length(), current[0].length()-1);
                if(!current[1].startsWith("\"action\":\"")) throw e;
                String action = current[1].substring("\"action\":\"".length(), current[1].length()-1);
                if(!current[2].startsWith("\"name\":\"")) throw e;
                String step = current[2].substring("\"step\":\"".length(), current[2].length()-1);

                //check if we're already good
                if("frozen".equals(phase) && "complete".equals(action) && "complete".equals(step)) return true;

                //try one more time with the current state
                MoveToStepRequest retryRequest = new MoveToStepRequest.Builder()
                    .index("fixed-partial-" + index.indexName)
                    .currentStep(b -> b.phase("cold").action(action).name(step))
                    .nextStep(b -> b.phase("frozen").action("complete").name("complete"))
                    .build();
                client.ilm().moveToStep(retryRequest);
                return true;
            }
            throw e;
        }
    }

    private boolean mountPartialSnapshot(ElasticsearchClient client, IndexToFix index) throws IOException {
        String mountRequest = "POST /_snapshot/" + oldRepo + "/" + index.snapshotName + "/_mount?storage=shared_cache&wait_for_completion=true\n" +
                "{\n" +
                "  \"index\": \"" + index.originalIndexName + "\", \n" +
                "  \"renamed_index\": \"fixed-partial-" + index.indexName + "\"\n" +
                "}";
        logger.info(mountRequest);

        try {
            MountRequest request = new MountRequest.Builder()
                    .repository(oldRepo)
                    .snapshot(index.snapshotName)
                    .storage("shared_cache")
                    .waitForCompletion(true)
                    .index(index.originalIndexName)
                    .renamedIndex("fixed-partial-" + index.indexName)
                    .build();
            MountResponse response = client.searchableSnapshots().mount(request);
            return true;
        } catch(ResponseException e) {
            // Check if we're good anyways
            if(e.getMessage().contains("cannot restore index [fixed-partial-" + index.indexName + "] because an open index with same name already exists in the cluster.")) {
                // Already mounted, perhaps there was an error / timeout before
                return true;
            }
            throw e;
        }
    }


    private static void updateLogging(boolean[] verbose) {
        if(logger instanceof ch.qos.logback.classic.Logger) {
            LoggerContext context = ((ch.qos.logback.classic.Logger) logger).getLoggerContext();
            context.getLogger("co.elastic.michael.bischoff.fixilm").setLevel(Level.WARN);
            int verbosity = 0;
            for(boolean entry : verbose) {
                if(entry) verbosity++;
                else verbosity--;
            }

            if(verbosity>=1) {
                context.getLogger("co.elastic.michael.bischoff.fixilm").setLevel(Level.INFO);
            }
            if(verbosity>=2) {
                context.getLogger("co.elastic.michael.bischoff.fixilm").setLevel(Level.DEBUG);
            }
        }
    }

    private record IndexToFix(String indexName, String originalIndexName, String phase, String action, String step, String snapshotName, String ilmPolicy, String datastream) { }
}