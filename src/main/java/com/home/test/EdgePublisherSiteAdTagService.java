package com.home.test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class EdgePublisherSiteAdTagService {

	public static void pushDataIntoDatabase() {
		OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance()
				.getOrientGraph(true);
		List<Map<String, Object>> pubAdTagSiteData = new FirstExample()
				.getData(
						"select id,name,site_id,pub_id from publisher_site_ad limit 7000,2500",
						Constants.PUBLISHER_AD_TAG.toLowerCase());

		// Map map = new HashMap();
		// OGlobalConfiguration.dumpConfiguration(System.out);
		// OGlobalConfiguration.setConfiguration(map);
		// OGlobalConfiguration.dumpConfiguration(System.out);
		 VertexUtility.dropClass(graphNoTx,
		 Constants.PUBLISHER_PUBLISHER_SITE);
		 VertexUtility.dropClass(graphNoTx, Constants.PUBLISHER_SITE_AD_TAG);
		 createClass(graphNoTx, Constants.PUBLISHER_PUBLISHER_SITE);
		 createClass(graphNoTx, Constants.PUBLISHER_SITE_AD_TAG);

//		createEdgeWithValidation(graphNoTx, pubAdTagSiteData);
	}

	private static void createEdgeWithValidation(OrientBaseGraph graphNoTx,
			List<Map<String, Object>> pubAdTagSiteData) {
		OrientVertex siteVertex;
		OrientVertex adTagVertex;
		OrientVertex publisherVertex;
		int count = 0;
		long start = System.currentTimeMillis();
		Set<String> pubSet = new HashSet<String>();
		((OrientGraph) graphNoTx).begin();
		if (pubAdTagSiteData.size() > 0) {
			for (Map<String, Object> pubAdTagSite : pubAdTagSiteData) {
				adTagVertex = VertexUtility.getVertex(graphNoTx,
						(String) pubAdTagSite.get("publisheradtag_id"),
						"adTagId", Constants.PUBLISHER_AD_TAG);
				siteVertex = VertexUtility.getVertex(graphNoTx,
						(String) pubAdTagSite.get("site_id"), "siteId",
						Constants.PUBLISHER_SITE);
				publisherVertex = VertexUtility.getVertex(graphNoTx,
						(String) pubAdTagSite.get("pub_id"), "publisherId",
						Constants.PUBLISHER);
				if (siteVertex != null
						&& adTagVertex != null
						&& !VertexUtility.isDirectedEdgePresent(graphNoTx,
								siteVertex.getIdentity().toString(),
								adTagVertex.getIdentity().toString(),
								Constants.PUBLISHER_SITE_AD_TAG)) {
					// ((OrientGraph) graphNoTx).begin();
					insertEdge(graphNoTx, siteVertex, adTagVertex,
							Constants.PUBLISHER_SITE_AD_TAG);
					// ((OrientGraph) graphNoTx).commit();
					count++;
					System.out.println(count + " "
							+ (System.currentTimeMillis() - start));
				}
				// if (siteVertex != null
				// && publisherVertex != null
				// && !VertexUtility.isDirectedEdgePresent(graphNoTx,
				// publisherVertex.getIdentity().toString(),
				// siteVertex.getIdentity().toString(),
				// Constants.PUBLISHER_PUBLISHER_SITE)) {
				// // ((OrientGraph) graphNoTx).begin();
				// insertEdge(graphNoTx, publisherVertex, siteVertex,
				// Constants.PUBLISHER_PUBLISHER_SITE);
				// // ((OrientGraph) graphNoTx).commit();
				count++;
				// }
			}
		}
		((OrientGraph) graphNoTx).commit();
		System.out.println(count + " edges added. "
				+ (System.currentTimeMillis() - start) + " pubSet : "
				+ pubSet.size());
	}

	private static void insertEdge(OrientBaseGraph graphNoTx,
			final OrientVertex siteVertex, final OrientVertex adTagVertex,
			final String edgeName) {
		if (graphNoTx instanceof OrientGraph) {
			((OrientGraph) graphNoTx).begin();
			siteVertex.addEdge(edgeName, adTagVertex);
			((OrientGraph) graphNoTx).commit();
		} else {
			siteVertex.addEdge(edgeName, adTagVertex);
		}
	}

	public static void createClass(final OrientBaseGraph graphNoTx,
			final String className) {
		if (graphNoTx instanceof OrientGraph) {
			((OrientGraph) graphNoTx)
					.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
						public Object call(OrientBaseGraph iArgument) {
							OClass publisherAdTag = graphNoTx
									.getRawGraph()
									.getMetadata()
									.getSchema()
									.createClass(
											className,
											(OClass) graphNoTx
													.getEdgeBaseType());
							publisherAdTag.createProperty("startDate",
									OType.DATETIME);
							publisherAdTag.createProperty("endDate",
									OType.DATETIME);
							graphNoTx.getRawGraph().getMetadata().getSchema()
									.save();
							return null;
						}
					});
		} else {
			OClass publisherAdTag = graphNoTx
					.getRawGraph()
					.getMetadata()
					.getSchema()
					.createClass(className,
							(OClass) graphNoTx.getEdgeBaseType());
			publisherAdTag.createProperty("startDate", OType.DATETIME);
			publisherAdTag.createProperty("endDate", OType.DATETIME);
			graphNoTx.getRawGraph().getMetadata().getSchema().save();
		}
	}

	public static void main(String[] args) {
//		fetchEdges();
		pushDataIntoDatabase();
	}

	public static void fetchEdges(/*OrientBaseGraph graphNoTx, String className*/) {
		OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance()
				.getOrientGraph(true);
		long tms = System.currentTimeMillis();
		Iterable<Edge> edges = graphNoTx
				.getEdgesOfClass(Constants.PUBLISHER_SITE_AD_TAG);
		System.out.println("Edges fetched time : "+(System.currentTimeMillis()-tms));
		Iterator<Edge> edgeList = edges.iterator();
		int count =0;
		while(edgeList.hasNext()){
			edgeList.next();
			count++;
		}
		System.out.println("total edge fetched : "+count);
	}

	@SuppressWarnings("unused")
	private static void createEdge(OrientBaseGraph graphNoTx,
			List<Map<String, Object>> pubAdTagSiteData) {
		int count = 0;
		long start = System.currentTimeMillis();
		if (pubAdTagSiteData.size() > 0) {
			for (Map<String, Object> pubAdTagSite : pubAdTagSiteData) {
				boolean isCreated = VertexUtility.createEdgeUsingSQL(
						graphNoTx,
						Constants.PUBLISHER_PUBLISHER_SITE,
						new String[] { Constants.PUBLISHER, "publisher_id",
								(String) pubAdTagSite.get("pub_id") },
						new String[] { Constants.PUBLISHER_SITE,
								"publishersite_id",
								(String) pubAdTagSite.get("site_id") });
				if (isCreated)
					count++;
				isCreated = VertexUtility.createEdgeUsingSQL(graphNoTx,
						Constants.PUBLISHER_SITE_AD_TAG, new String[] {
								Constants.PUBLISHER_SITE, "publishersite_id",
								(String) pubAdTagSite.get("site_id") },
						new String[] { Constants.PUBLISHER_AD_TAG,
								"publisheradtag_id", "publisheradtsg_id" });
				if (isCreated)
					count++;
			}
		}
		System.out.println(count + " edges added. "
				+ (System.currentTimeMillis() - start));
	}
}
