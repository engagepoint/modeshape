package org.modeshape.jcr;

import java.util.ArrayList;
import java.util.List;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * See MODE-2174: JCR Query in Modeshape returns no result when empty string used in comparison
 * @author: vadym.karko
 * @since: 3/21/14 12:35 PM
 */
public class QueryEmptyStringTest extends MultiUseAbstractTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String SQL = "SELECT [jcr:name], [car:model], [car:maker] FROM [car:Car] ";


    @BeforeClass
    public static void setUp() throws Exception
    {
        RepositoryConfiguration config = new RepositoryConfiguration("config/simple-repo-config.json");
        startRepository(config);
        registerNodeTypes("cnd/cars.cnd");

        Node root = session.getRootNode();
        Node item;

        item = root.addNode("Aston Martin", "car:Car");
        item.setProperty("car:maker", "Aston Martin");
        item.setProperty("car:model", "DB9");

        item = root.addNode("Infiniti", "car:Car");
        item.setProperty("car:maker", "Infiniti");

        item = root.addNode("EMPTY", "car:Car");
        item.setProperty("car:maker", "");

        item = root.addNode("NULL", "car:Car");

        /**
         * jcr:name          |    car:maker         |    car:model
         * -----------------------------------------------------------
         * 'Aston Martin'    |    'Aston Martin'    |    'DB9'
         * 'Infiniti'        |    'Infiniti'        |    null
         * 'EMPTY'           |    ''                |    null
         * 'NULL'            |    null              |    null
         */

        session.save();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        stopRepository();
    }

    @Before
    public void before() throws RepositoryException
    {
        logger.info("\nBefore:");

        Query query = session.getWorkspace().getQueryManager().createQuery(SQL, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);
    }

    private List<String> collectNames(QueryResult result) throws RepositoryException
    {
        NodeIterator iterator = result.getNodes();
        List<String> actual = new ArrayList<String>();
        while (iterator.hasNext()) actual.add(iterator.nextNode().getName());

        return actual;
    }

    @Override
    protected void printResults(QueryResult results)
    {
        print = true;
        super.printResults(results);
    }


    @Test
    public void shouldEqualsEmpty() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] = ''";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }

    @Test
    public void shouldIsNotNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NOT NULL";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 3, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
        Assert.assertTrue("Should contains Infiniti", actual.contains("Infiniti"));
    }

    @Test
    public void shouldIsNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NULL";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains NULL", actual.contains("NULL"));
    }

    @Test
    public void shouldLengthEqualsZero() throws Exception
    {
        String sql = SQL + "WHERE LENGTH([car:maker]) = 0";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }

    @Test
    public void shouldLengthGreaterZero() throws Exception {
        String sql = SQL + "WHERE LENGTH([car:maker]) > 0";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(2, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
        Assert.assertTrue("Should contains Infiniti", actual.contains("Infiniti"));
    }

    @Test
    public void shouldLengthGreaterOrEqualsZero() throws Exception {
        String sql = SQL + "WHERE LENGTH([car:maker]) >= 0";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(3, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
        Assert.assertTrue("Should contains Infiniti", actual.contains("Infiniti"));
    }

    @Test
    public void shouldLikeEmpty() throws Exception {
        String sql = SQL + "WHERE [car:maker] LIKE ''";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }

    @Test
    public void shouldInEmpty() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IN ('')";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals(1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }

    @Test
    public void shouldInEmptyAndString() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IN ('Aston Martin', '')";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);


        Assert.assertEquals("Should contains rows", 2, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
    }

    @Test
    public void shouldOrEqualsEmpty() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] = '' OR [car:maker] = 'Infiniti'";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);


        Assert.assertEquals("Should contains rows", 2, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
        Assert.assertTrue("Should contains Infiniti", actual.contains("Infiniti"));
    }

    @Test
    public void shouldOrIsNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NULL OR [car:model] = 'DB9'";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);


        Assert.assertEquals("Should contains rows", 2, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
        Assert.assertTrue("Should contains NULL", actual.contains("NULL"));
    }

    @Test
    public void shouldOrIsNotNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NOT NULL OR [car:model] = 'DB9'";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);


        Assert.assertEquals("Should contains rows", 3, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
        Assert.assertTrue("Should contains Infiniti", actual.contains("Infiniti"));
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
    }

    @Test
    public void shouldAndEqualsEmpty() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] = '' AND [car:model] IS NULL";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }

    @Test
    public void shouldAndIsNotNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NOT NULL AND [car:model] = 'DB9'";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));
    }

    @Test
    public void shouldAndIsNull() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IS NULL AND [car:model] IS NULL";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains NULL", actual.contains("NULL"));
    }

    @Test
    public void shouldPaging() throws Exception
    {
        String sql = SQL + "WHERE [car:maker] IN ('Aston Martin', '') ORDER BY [jcr:name]";
        logger.info("\nSQL: {}", sql);

        Query query = session.getWorkspace().getQueryManager().createQuery(sql, Query.JCR_SQL2);
        query.setLimit(1);

        logger.info("PAGE 1:");
        query.setOffset(0);
        QueryResult result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        List<String> actual = collectNames(result);
        Assert.assertTrue("Should contains Aston Martin", actual.contains("Aston Martin"));


        logger.info("PAGE 2:");
        query.setOffset(1);
        result = query.execute();

        printResults(result);

        Assert.assertEquals("Should contains rows", 1, result.getRows().getSize());
        actual = collectNames(result);
        Assert.assertTrue("Should contains EMPTY", actual.contains("EMPTY"));
    }
}