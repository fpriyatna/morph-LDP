package es.upm.fi.dia.oeg.morph.r2rml.ldp.engine

import scala.collection.JavaConversions._
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import java.io.Writer
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.OpAsQuery
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBDataTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import Zql.ZUpdate

class MorphLDPRunner (mappingDocument:R2RMLMappingDocument
//    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphRDBUnfolder
    , dataTranslator:Option[MorphRDBDataTranslator]
//    , materializer:MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    , outputStream:Writer
    ) extends MorphRDBRunner(mappingDocument
//    , dataSourceReader
    , unfolder
    , dataTranslator
//    , materializer
    , queryTranslator
    , resultProcessor
    , outputStream
        ) {
  
	override val logger = Logger.getLogger(this.getClass());
	
	def materializeLDPRequest(ldpRequest:String) = {
	  if(ldpRequest.endsWith("/")) {
	    logger.info("Materializing LDPC");
	    this.materializeContainer(ldpRequest);
	  } else {
	    logger.info("Materializing LDPR");
	    this.materializeResource(ldpRequest);
	  }
	}	

	def materializeContainer(containerValue:String) = {
		val cms = this.mappingDocument.getClassMappingsByInstanceTemplate(containerValue);
		this.materializeClassMappings(cms);
	}

	def materializeResource(instanceURI:String) = {
	  val cms = this.mappingDocument.getClassMappingsByInstanceURI(instanceURI);
	  this.materializeInstanceDetails(instanceURI, cms)
	}
	


	def queryContainer(ldpRequest:String) = {
		val cms = this.mappingDocument.getClassMappingsByInstanceTemplate(ldpRequest);
		cms.foreach(cm => {
			val subjectVariable = "s";
			val classURI = cm.getMappedClassURIs.iterator.next;
			val tpSubject = NodeFactory.createVariable(subjectVariable);
			val tpPredicate1 = RDF.`type`.asNode();
			val tpObject1 = NodeFactory.createURI(classURI);
			val tp1 = new Triple(tpSubject, tpPredicate1, tpObject1);
			val tpObject2 = RDFS.Resource.asNode();
			val tp2 = new Triple(tpSubject, tpPredicate1, tpObject2);//FORCING STG
			val basicPattern = BasicPattern.wrap(List(tp1, tp2));
			val bgp = new OpBGP(basicPattern);
			val varSubject = Var.alloc(subjectVariable);
			val opProject = new OpProject(bgp, List(varSubject));
			val sparqlQuery = OpAsQuery.asQuery(opProject);
			sparqlQuery.setQuerySelectType();
			val mapSparqlSQL = this.translateSPARQLQueriesIntoSQLQueries(List(sparqlQuery));
			this.queryResultTranslator.get.translateResult(mapSparqlSQL);				
		})
	}
	
	def queryResource(ldpRequest:String) = {
	  val cms = this.mappingDocument.getClassMappingsByInstanceURI(ldpRequest);
	  cms.foreach(cm => {
		  val classURI = cm.getMappedClassURIs.iterator.next;
		  val stgSubject = NodeFactory.createURI(ldpRequest);
		  val tp1Predicate = RDF.`type`.asNode();
		  val tp1Object = NodeFactory.createURI(classURI);
		  val tp1 = new Triple(stgSubject, tp1Predicate, tp1Object);

		  val tp2Predicate = NodeFactory.createVariable("p");
		  val tp2Object = NodeFactory.createVariable("o");
		  val tp2 = new Triple(stgSubject, tp2Predicate, tp2Object);
			
		  val basicPattern = BasicPattern.wrap(List(tp1, tp2));
		  val bgp = new OpBGP(basicPattern);
		  val varPredicate = Var.alloc(tp2Predicate);
		  val varObject = Var.alloc(tp2Object);
		  
		  val opProject = new OpProject(bgp, List(varPredicate, varObject));
		  val sparqlQuery = OpAsQuery.asQuery(opProject);
		  sparqlQuery.setQuerySelectType();
		  val mapSparqlSQL = this.translateSPARQLQueriesIntoSQLQueries(List(sparqlQuery));
		  this.queryResultTranslator.get.translateResult(mapSparqlSQL);	    
	  })

	}

	def updateResource(resource:Resource) : Unit = {
		val statements = resource.listProperties().toList();
		this.updateResource(statements);
	}
	
	def updateResource(statements:Iterable[Statement]) : Unit = {
		val triples = statements.map(stmt => {
			val obj = stmt.getObject().asNode()
			val triple = Triple.create(stmt.getSubject().asNode(), stmt.getPredicate().asNode(), stmt.getObject().asNode());
			triple 
		})
		val basicPattern = BasicPattern.wrap(triples.toList);
		val bgp = new OpBGP(basicPattern);
		//val mbQueryTranslator = this.queryTranslator.get.asInstanceOf[MorphBaseQueryTranslator];
		//val zUpdate = mbQueryTranslator.translateUpdate(bgp)
		val zUpdate = this.queryTranslator.get.translateUpdate(bgp)
		logger.info("zUpdate = \n" + zUpdate);
		val dataSourceReader = this.dataTranslator.get.getDataSourceReader;
		dataSourceReader.execute(zUpdate.toString());
	}	

	def createResource(resource:Resource) : Unit = {
		val statements = resource.listProperties().toList();
		this.createResource(statements);
	}
	
	def createResource(statements:Iterable[Statement]) : Unit = {
		val triples = statements.map(stmt => {
			val obj = stmt.getObject().asNode()
			val triple = Triple.create(stmt.getSubject().asNode(), stmt.getPredicate().asNode(), stmt.getObject().asNode());
			triple 
		})
		val basicPattern = BasicPattern.wrap(triples.toList);
		val bgp = new OpBGP(basicPattern);
		//val mbQueryTranslator = this.queryTranslator.get.asInstanceOf[MorphBaseQueryTranslator];
		//val zUpdate = mbQueryTranslator.translateUpdate(bgp)
		val zInsert = this.queryTranslator.get.translateInsert(bgp)
		logger.info("zInsert = \n" + zInsert);
		val dataSourceReader = this.dataTranslator.get.getDataSourceReader;
		dataSourceReader.execute(zInsert.toString());
	}	

	def deleteResource(resourceURI:String) = {
	  val cms = this.mappingDocument.getClassMappingsByInstanceURI(resourceURI);
	  cms.foreach(cm => {
		  val classURI = cm.getMappedClassURIs.iterator.next;
		  val stgSubject = NodeFactory.createURI(resourceURI);
		  val tp1Predicate = RDF.`type`.asNode();
		  val tp1Object = NodeFactory.createURI(classURI);
		  val tp1 = new Triple(stgSubject, tp1Predicate, tp1Object);

		  val tp2Predicate = NodeFactory.createVariable("p");
		  val tp2Object = NodeFactory.createVariable("o");
		  val tp2 = new Triple(stgSubject, tp2Predicate, tp2Object);
			
		  //val basicPattern = BasicPattern.wrap(List(tp1, tp2));
		  val basicPattern = BasicPattern.wrap(List(tp1));
		  val bgp = new OpBGP(basicPattern);
		  val varPredicate = Var.alloc(tp2Predicate);
		  val varObject = Var.alloc(tp2Object);
		  
		  val zDelete = this.queryTranslator.get.translateDelete(bgp);
		  logger.info("zDelete = \n" + zDelete);
		  val dataSourceReader = this.dataTranslator.get.getDataSourceReader;
		  dataSourceReader.execute(zDelete.toString());
	  })

	}
}