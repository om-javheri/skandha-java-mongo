package site;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import tools.jackson.databind.json.JsonMapper;

/** Form messages and daily page-view counters in MongoDB (DATABASE_URL). Nothing about visitors is kept. */
public final class MongoStore implements Store {
    private static final DateTimeFormatter CREATED = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final MongoCollection<Document> messages;
    private final MongoCollection<Document> counts;
    private final MongoCollection<Document> counters;
    private final MongoCollection<Document> hits;
    private final int keepMessages;
    private final JsonMapper json;

    public MongoStore(String url, int keepMessages, JsonMapper json) {
        ConnectionString address = new ConnectionString(url);
        MongoClient client = MongoClients.create(address);
        MongoDatabase db = client.getDatabase(address.getDatabase() == null ? "site" : address.getDatabase());
        messages = db.getCollection("form_messages");
        counts = db.getCollection("daily_counts");
        counters = db.getCollection("counters");
        hits = db.getCollection("rate_hits");
        this.keepMessages = keepMessages;
        this.json = json;
        counts.createIndex(Indexes.ascending("day", "kind", "key"), new IndexOptions().unique(true));
        // Hits older than a minute go by themselves (counting looks at the last minute only).
        hits.createIndex(Indexes.ascending("at"), new IndexOptions().expireAfter(60L, TimeUnit.SECONDS));
        hits.createIndex(Indexes.ascending("bucket", "at"));
    }

    private static String today(int offset) {
        return LocalDate.now(ZoneOffset.UTC).plusDays(offset).toString();
    }

    @Override
    public void addMessage(Rules.Message m) throws Exception {
        Document next = counters.findOneAndUpdate(Filters.eq("_id", "form_messages"), Updates.inc("seq", 1), new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        long id = ((Number) next.get("seq")).longValue();
        List<Document> fields = new ArrayList<>();
        for (Rules.Field f : m.fields()) fields.add(new Document("label", f.label()).append("value", f.value()));
        messages.insertOne(new Document("_id", id).append("formId", m.formId()).append("page", m.page()).append("fields", fields).append("createdAt", CREATED.format(Instant.now().truncatedTo(ChronoUnit.MILLIS))));
        // Only the newest are kept.
        Document oldest = messages.find().sort(Sorts.descending("_id")).skip(keepMessages).limit(1).first();
        if (oldest != null) messages.deleteMany(Filters.lte("_id", oldest.get("_id")));
    }

    @Override
    public List<Map<String, Object>> messages(int limit) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Document d : messages.find().sort(Sorts.descending("_id")).limit(limit)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", ((Number) d.get("_id")).longValue());
            m.put("formId", d.getString("formId"));
            m.put("page", d.getString("page"));
            List<Map<String, Object>> fields = new ArrayList<>();
            for (Document f : d.getList("fields", Document.class)) {
                Map<String, Object> field = new LinkedHashMap<>();
                field.put("label", f.getString("label"));
                field.put("value", f.getString("value"));
                fields.add(field);
            }
            m.put("fields", fields);
            m.put("createdAt", d.getString("createdAt"));
            out.add(m);
        }
        return out;
    }

    @Override
    public boolean deleteMessage(long id) {
        return messages.deleteOne(Filters.eq("_id", id)).getDeletedCount() > 0;
    }

    @Override
    public void countView(String path, String referrer) {
        UpdateOptions upsert = new UpdateOptions().upsert(true);
        counts.updateOne(Filters.and(Filters.eq("day", today(0)), Filters.eq("kind", "view"), Filters.eq("key", path)), Updates.inc("count", 1), upsert);
        if (referrer != null) counts.updateOne(Filters.and(Filters.eq("day", today(0)), Filters.eq("kind", "referrer"), Filters.eq("key", referrer)), Updates.inc("count", 1), upsert);
    }

    private List<Map<String, Object>> totals(String kind, String since) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Document d : counts.aggregate(List.of(Aggregates.match(Filters.and(Filters.eq("kind", kind), Filters.gte("day", since))), Aggregates.group("$key", Accumulators.sum("count", "$count")), Aggregates.sort(Sorts.orderBy(Sorts.descending("count"), Sorts.ascending("_id")))))) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("key", d.getString("_id"));
            t.put("count", ((Number) d.get("count")).longValue());
            out.add(t);
        }
        return out;
    }

    @Override
    public Map<String, Object> stats(int days) {
        String since = today(-(days - 1));
        List<Map<String, Object>> daily = new ArrayList<>();
        for (Document d : counts.aggregate(List.of(Aggregates.match(Filters.and(Filters.eq("kind", "view"), Filters.gte("day", since))), Aggregates.group("$day", Accumulators.sum("views", "$count")), Aggregates.sort(Sorts.ascending("_id"))))) {
            Map<String, Object> day = new LinkedHashMap<>();
            day.put("day", d.getString("_id"));
            day.put("views", ((Number) d.get("views")).longValue());
            daily.add(day);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("days", days);
        out.put("daily", daily);
        out.put("pages", totals("view", since));
        out.put("referrers", totals("referrer", since));
        return out;
    }

    @Override
    public Boolean allow(String bucket, int perMinute) {
        Date since = new Date(System.currentTimeMillis() - 60_000);
        if (hits.countDocuments(Filters.and(Filters.eq("bucket", bucket), Filters.gt("at", since))) >= perMinute) return false;
        hits.insertOne(new Document("bucket", bucket).append("at", new Date()));
        return true;
    }
}
