package es.upm.fi.dia.oeg.morph.r2rml.ldp.engine

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import java.io.Writer
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBDataTranslator

class MorphLDPRunnerFactory extends MorphRDBRunnerFactory {
	override def createRunner(mappingDocument:MorphBaseMappingDocument
//    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :Option[MorphBaseDataTranslator]
//    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
	, outputStream:Writer
    ) : MorphLDPRunner = { 
	  new MorphLDPRunner(mappingDocument.asInstanceOf[R2RMLMappingDocument]
//    , dataSourceReader
    , unfolder.asInstanceOf[MorphRDBUnfolder]
    , dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]]
//    , materializer
    , queryTranslator
    , resultProcessor
    , outputStream
    )
	}
}