package com.microsoft.azure.cosmosdb.sql.jsonstoreimport;

import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

public interface IPartitionKeyComponent {

	 int CompareTo(IPartitionKeyComponent other);

    int GetTypeOrdinal();

    void JsonEncode(JsonGenerator writer);

    void WriteForHashing(OutputStream outputStream);

    void WriteForBinaryEncoding(OutputStream outputStream);

    void WriteForHashingV2(OutputStream binaryWriter);

    IPartitionKeyComponent Truncate();
}
