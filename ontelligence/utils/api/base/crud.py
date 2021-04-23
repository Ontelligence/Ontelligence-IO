from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from sqlalchemy import Column, DateTime, String, Integer, Boolean, and_, text

from app.extensions import db, marshmallow as ma
from app.utils.db import BaseModel
# from app.utils.errors.sqlalchemy import NonexistentIdentityError  # IdentityPermissionDenied
from app.utils.db import NonexistentIdentityError, IdentityPermissionDenied

from fastapi.encoders import jsonable_encoder
import pydantic
from sqlalchemy.orm import Session
from sqlalchemy import Column, DateTime, String, Integer, Boolean, and_, text

from ontelligence.utils.api.base.sqlalchemy import BaseModel
from app.utils.errors.sqlalchemy import NonexistentIdentityError  # IdentityPermissionDenied


ModelType = TypeVar("ModelType", bound=BaseModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=pydantic.BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=pydantic.BaseModel)



class BaseCRUD(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):

    def __init__(self, model: Type[ModelType]):
        """CRUD object with default methods to Create, Read, Update, Delete (CRUD)"""
        self.model = model

    def _build_query(
            self,
            db: Session,
            query: Dict[str, Any] = None,
            skip: int = None,
            limit: int = None,
            sort_keys: Dict[str, str] = None
    ) -> Any:
        if not query:
            return db.query(self.model).filter(text(''))

        aggregate_types = {
            String: str,
            Integer: int,
            Boolean: bool,
        }

        # Create filter chain.
        chain = set()
        for _field, _value in query.items():
            field = getattr(self.model, _field)
            agg = aggregate_types.get(type(field.type))
            if not agg:
                raise Exception(f'Unmapped type: {type(field.type)}')
            try:
                value = agg(_value)
            except ValueError:
                raise ValueError(f'{field} requires a value of type {agg} but received {repr(_value)}.')
            chain.add(field == value)

        filter_expression = and_(*chain)
        _query = db.query(self.model).filter(filter_expression)
        if sort_keys:
            for sort_key, sort_order in sort_keys.items():
                _query = _query.order_by(getattr(getattr(self.model, sort_key), sort_order)())
        if skip:
            _query = _query.offset(skip)
        if limit:
            _query = _query.limit(limit)
        return _query

    # TODO: Filter items based on current user's permissions.
    # def _validate_client_access(self, items):
    #     has_access = False
    #     if hasattr(items, '_has_access'):
    #         has_access = items._has_access(current_user)
    #     else:
    #         raise Exception(f'{cls().__class__.__name__} does not have _has_access() defined')
    #
    #     return has_access

    def find_one(
            self,
            db: Session,
            query: Dict[str, Any] = None,
            return_query: bool = False,
            raise_not_found: bool = True,
            raise_permission_denied: bool = True,
            skip: int = None,
            limit: int = None,
            sort_keys: Dict[str, str] = None
    ) -> Optional[ModelType]:
        """Return one SQLAlchemy model instance"""
        def f():
            _query = self._build_query(db=db, query=query, skip=skip, limit=limit, sort_keys=sort_keys)
            items = _query.first()

            if raise_not_found and not items:
                raise NonexistentIdentityError(self.model)

            # TODO: Filter items based on current user's permissions.
            # if current_user and raise_permission_denied and not cls._validate_client_access(items):
            #     raise IdentityPermissionDenied(cls)

            if return_query:
                return _query
            return items

        return f()

    def find_many(
            self,
            db: Session,
            query: Dict[str, Any] = None,
            return_query: bool = False,
            raise_not_found: bool = True,
            raise_permission_denied: bool = True,
            skip: int = None,
            limit: int = None,
            sort_keys: Dict[str, str] = None
    ) -> List[ModelType]:
        """Return multiple SQLAlchemy model instances"""

        def f():
            _query = self._build_query(db=db, query=query, skip=skip, limit=limit, sort_keys=sort_keys)
            items = _query.all()

            # TODO: Filter items based on current user's permissions.
            # if current_user:
            #     items = [x for x in items if cls._validate_client_access(x)]

            if return_query:
                return _query
            return items

        return f()

    def create_one(self,db: Session, data: Union[CreateSchemaType, Dict[str, Any]]) -> ModelType:
        """Create a new SQLAlchemy model instance"""

        if isinstance(data, dict) and hasattr(CreateSchemaType, 'from_dict'):
            schema_obj = CreateSchemaType.from_dict(data)
            data = schema_obj.dict()

        db_obj = self.model(**data)  # type: ignore
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update_one(
            self,
            db: Session,
            query: Dict[str, Any],
            data: Union[UpdateSchemaType, Dict[str, Any]]
    ) -> ModelType:
        """Update one SQLAlchemy model instance"""

        # TODO: from fastapi.encoders import jsonable_encoder;
        #       obj_in_data = jsonable_encoder(obj_in)

        data = self.update_schema.load(data)
        _query = self.find_one(db=db, query=query, return_query=True)
        result = _query.update(data, synchronize_session='fetch')
        if result:
            db.commit()
        return _query.first()

    def delete_one(
            self,
            db: Session,
            query: Dict[str, Any]
    ) -> ModelType:
        """Delete one SQLAlchemy model instance"""
        _query = self.find_one(db=db, query=query, return_query=True)
        count = _query.delete(synchronize_session=False)
        if count:
            db.commit()
        return count

    def delete_many(
            self,
            db: Session,
            query: Dict[str, Any]
    ) -> int:
        """Delete multiple SQLAlchemy model instances"""
        _query = self.find_many(db=db, query=query, return_query=True)
        count = _query.delete(synchronize_session=False)
        if count:
            db.commit()
        return count

    # # TODO: Update relationship: Dict to match {model_field: {related_field: related_value}}.
    # def find_relationship(self, db: Session, query: Dict = None, relationship: Dict = None) -> Any:
    #     item = self.find_one(db=db, query=query)
    #     for key, val in relationship.items():
    #         if hasattr(item, key):
    #             related_items = [x for x in getattr(item, key) if all(getattr(x, k) == v for k, v in val.items())]
    #             if len(related_items) > 0:
    #                 return related_items[0]
    #     return None
    #
    # def add_related(self, db: Session, query: Dict = None, relationship: Dict = None) -> Any:
    #     item = self.find_one(db=db, query=query)
    #     for key, val in relationship.items():
    #         if hasattr(item, key):
    #             related_field = getattr(item, key)
    #             related_field.append(val)
    #             db.add(related_field)
    #             db.commit()
    #             db.refresh(related_field)
    #             return related_field
    #     return None

    # def get(self, db: Session, id: Any) -> Optional[ModelType]:
    #     return db.query(self.model).filter(self.model.id == id).first()

    # def get_multi(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[ModelType]:
    #     return db.query(self.model).offset(skip).limit(limit).all()

    # def create(self, db: Session, *, obj_in: CreateSchemaType) -> ModelType:
    #     obj_in_data = jsonable_encoder(obj_in)
    #     db_obj = self.model(**obj_in_data)  # type: ignore
    #     db.add(db_obj)
    #     db.commit()
    #     db.refresh(db_obj)
    #     return db_obj

    # def update(self, db: Session, *, db_obj: ModelType, obj_in: Union[UpdateSchemaType, Dict[str, Any]]) -> ModelType:
    #     obj_data = jsonable_encoder(db_obj)
    #     if isinstance(obj_in, dict):
    #         update_data = obj_in
    #     else:
    #         update_data = obj_in.dict(exclude_unset=True)
    #     for field in obj_data:
    #         if field in update_data:
    #             setattr(db_obj, field, update_data[field])
    #     db.add(db_obj)
    #     db.commit()
    #     db.refresh(db_obj)
    #     return db_obj

    # def remove(self, db: Session, *, id: int) -> ModelType:
    #     obj = db.query(self.model).get(id)
    #     db.delete(obj)
    #     db.commit()
    #     return obj
