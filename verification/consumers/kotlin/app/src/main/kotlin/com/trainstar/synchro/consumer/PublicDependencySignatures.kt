package com.trainstar.synchro.consumer

import com.trainstar.synchro.ChangeRecord
import com.trainstar.synchro.HttpClient
import com.trainstar.synchro.SynchroConfig
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonObject
import okhttp3.OkHttpClient

// This consumer declares no OkHttp or kotlinx.serialization dependency. It compiles only when the
// published Synchro metadata exports the libraries that these public signatures expose.
internal fun publicDependencySignatures(config: SynchroConfig, change: ChangeRecord): Triple<HttpClient, JsonObject, KSerializer<ChangeRecord>> =
    Triple(HttpClient(config, OkHttpClient()), change.pk, ChangeRecord.serializer())
