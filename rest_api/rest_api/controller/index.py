from typing import Optional, List

import json
import shutil
import uuid
from pathlib import Path

from fastapi import FastAPI, APIRouter, UploadFile, File, Form, HTTPException, Depends
from pydantic import BaseModel
from haystack import Pipeline
from haystack.document_stores import ElasticsearchDocumentStore
from haystack.nodes import FileTypeClassifier, MarkdownConverter, PDFToTextConverter, \
    TextConverter, PreProcessor, EmbeddingRetriever

from rest_api.utils import get_app, get_pipelines
from rest_api.config import FILE_UPLOAD_PATH
from rest_api.config import ELASTIC_HOST
from rest_api.config import ELASTIC_PASSWORD
from rest_api.controller.utils import as_form

router = APIRouter()
app: FastAPI = get_app()


@as_form
class FileConverterParams(BaseModel):
    remove_numeric_tables: Optional[bool] = None
    valid_languages: Optional[List[str]] = None


@as_form
class PreprocessorParams(BaseModel):
    clean_whitespace: Optional[bool] = None
    clean_empty_lines: Optional[bool] = None
    clean_header_footer: Optional[bool] = None
    split_by: Optional[str] = None
    split_length: Optional[int] = None
    split_overlap: Optional[int] = None
    split_respect_sentence_boundary: Optional[bool] = None


class Response(BaseModel):
    file_id: str


@router.post("/index")
def upload_file(
        files: List[UploadFile] = File(...),
        # JSON serialized string
        meta: Optional[str] = Form("null"),  # type: ignore
        fileconverter_params: FileConverterParams = Depends(FileConverterParams.as_form),  # type: ignore
        preprocessor_params: PreprocessorParams = Depends(PreprocessorParams.as_form),  # type: ignore
):
    file_paths: list = []
    file_metas: list = []

    meta_form = json.loads(meta) or {}  # type: ignore
    if not isinstance(meta_form, dict):
        raise HTTPException(status_code=500, detail=f"The meta field must be a dict or None, not {type(meta_form)}")

    for file in files:
        try:
            file_path = Path(FILE_UPLOAD_PATH) / f"{uuid.uuid4().hex}_{file.filename}"
            with file_path.open("wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            file_paths.append(file_path)
            meta_form["name"] = file.filename
            file_metas.append(meta_form)
        finally:
            file.file.close()

    document_store = ElasticsearchDocumentStore(
        similarity='dot_product',
        embedding_dim=768,
        host=ELASTIC_HOST,
        port=9243,
        username='elastic',
        password=ELASTIC_PASSWORD,
        scheme='https',
        ca_certs='/etc/ssl/certs/ca-certificates.crt',
        verify_certs=True,
        duplicate_documents='overwrite',
        index=file_metas[0]['index']
    )

    embedding_retriever = EmbeddingRetriever(
        document_store=document_store,
        embedding_model='sentence-transformers/multi-qa-mpnet-base-dot-v1',
        model_format='sentence_transformers'
    )

    file_type_classifier = FileTypeClassifier()
    text_converter = TextConverter()
    pdf_converter = PDFToTextConverter()
    md_converter = MarkdownConverter()

    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=True,
        split_by='sentence',
        split_length=50,
        split_respect_sentence_boundary=False,
        split_overlap=0
    )

    p = Pipeline()

    p.add_node(component=file_type_classifier, name='FileTypeClassifier', inputs=['File'])
    p.add_node(component=text_converter, name='TextConverter', inputs=['FileTypeClassifier.output_1'])
    p.add_node(component=pdf_converter, name='PDFToTextConverter', inputs=['FileTypeClassifier.output_2'])
    p.add_node(component=md_converter, name='MarkdownConverter', inputs=['FileTypeClassifier.output_3'])
    p.add_node(component=preprocessor, name='PreProcessor',
               inputs=['TextConverter', 'PDFToTextConverter', 'MarkdownConverter'])
    p.add_node(component=embedding_retriever, name='EmbeddingRetriever', inputs=['PreProcessor'])
    p.add_node(component=document_store, name='DocumentStore', inputs=['EmbeddingRetriever'])

    p.run(file_paths=file_paths, meta=file_metas)
