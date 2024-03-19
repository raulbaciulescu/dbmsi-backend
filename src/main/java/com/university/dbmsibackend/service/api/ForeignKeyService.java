package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.dto.CreateForeignKeyRequest;

public interface ForeignKeyService {
    void createForeignKey(CreateForeignKeyRequest request);
}
