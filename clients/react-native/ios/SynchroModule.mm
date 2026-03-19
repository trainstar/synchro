#import <React/RCTBridgeModule.h>
#import <React/RCTEventEmitter.h>

#ifdef RCT_NEW_ARCH_ENABLED
#import <SynchroModuleSpec/SynchroModuleSpec.h>
#endif

// Auto-generated Swift bridging header
#if __has_include("SynchroReactNative-Swift.h")
#import "SynchroReactNative-Swift.h"
#else
#import <SynchroReactNative/SynchroReactNative-Swift.h>
#endif

#ifdef RCT_NEW_ARCH_ENABLED

// ──────────────────────────────────────────────────────────────
// New Architecture: TurboModule conforming to Codegen spec
// ──────────────────────────────────────────────────────────────

@interface SynchroModule : NativeSynchroSpecBase <NativeSynchroSpec, SynchroEventEmitting>
@property (nonatomic, strong) SynchroModuleImpl *impl;
@end

@implementation SynchroModule

+ (NSString *)moduleName {
    return @"SynchroModule";
}

- (instancetype)init {
    self = [super init];
    if (self) {
        _impl = [[SynchroModuleImpl alloc] init];
        _impl.eventDelegate = self;
    }
    return self;
}

+ (BOOL)requiresMainQueueSetup {
    return NO;
}

- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:(const facebook::react::ObjCTurboModule::InitParams &)params {
    return std::make_shared<facebook::react::NativeSynchroSpecJSI>(params);
}

// MARK: - SynchroEventEmitting (event delegation from Swift impl)

- (void)emitEvent:(NSString *)name body:(NSDictionary *)body {
    if ([name isEqualToString:@"onStatusChange"]) {
        [self emitOnStatusChange:body];
    } else if ([name isEqualToString:@"onConflict"]) {
        [self emitOnConflict:body];
    } else if ([name isEqualToString:@"onAuthRequest"]) {
        [self emitOnAuthRequest:body];
    } else if ([name isEqualToString:@"onChange"]) {
        [self emitOnChange:body];
    } else if ([name isEqualToString:@"onQueryResult"]) {
        [self emitOnQueryResult:body];
    }
}

// MARK: - NativeSynchroSpec protocol methods

- (void)initialize:(JS::NativeSynchro::SpecInitializeConfig &)config
           resolve:(RCTPromiseResolveBlock)resolve
            reject:(RCTPromiseRejectBlock)reject {
    NSMutableDictionary *configDict = [@{
        @"dbPath": config.dbPath(),
        @"serverURL": config.serverURL(),
        @"clientID": config.clientID(),
        @"platform": config.platform(),
        @"appVersion": config.appVersion(),
        @"syncInterval": @(config.syncInterval()),
        @"pushDebounce": @(config.pushDebounce()),
        @"maxRetryAttempts": @(config.maxRetryAttempts()),
        @"pullPageSize": @(config.pullPageSize()),
        @"pushBatchSize": @(config.pushBatchSize()),
    } mutableCopy];
    NSString *seedPath = config.seedDatabasePath();
    if (seedPath) {
        configDict[@"seedDatabasePath"] = seedPath;
    }
    [self.impl initialize:configDict resolve:resolve reject:reject];
}

- (void)close:(RCTPromiseResolveBlock)resolve
       reject:(RCTPromiseRejectBlock)reject {
    [self.impl close:resolve reject:reject];
}

- (void)getPath:(RCTPromiseResolveBlock)resolve
         reject:(RCTPromiseRejectBlock)reject {
    [self.impl getPath:resolve reject:reject];
}

- (void)query:(NSString *)sql
   paramsJson:(NSString *)paramsJson
      resolve:(RCTPromiseResolveBlock)resolve
       reject:(RCTPromiseRejectBlock)reject {
    [self.impl query:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)queryOne:(NSString *)sql
      paramsJson:(NSString *)paramsJson
         resolve:(RCTPromiseResolveBlock)resolve
          reject:(RCTPromiseRejectBlock)reject {
    [self.impl queryOne:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)execute:(NSString *)sql
     paramsJson:(NSString *)paramsJson
        resolve:(RCTPromiseResolveBlock)resolve
         reject:(RCTPromiseRejectBlock)reject {
    [self.impl execute:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)executeBatch:(NSString *)statementsJson
             resolve:(RCTPromiseResolveBlock)resolve
              reject:(RCTPromiseRejectBlock)reject {
    [self.impl executeBatch:statementsJson resolve:resolve reject:reject];
}

- (void)beginWriteTransaction:(RCTPromiseResolveBlock)resolve
                       reject:(RCTPromiseRejectBlock)reject {
    [self.impl beginWriteTransaction:resolve reject:reject];
}

- (void)beginReadTransaction:(RCTPromiseResolveBlock)resolve
                      reject:(RCTPromiseRejectBlock)reject {
    [self.impl beginReadTransaction:resolve reject:reject];
}

- (void)txQuery:(NSString *)txID
            sql:(NSString *)sql
     paramsJson:(NSString *)paramsJson
        resolve:(RCTPromiseResolveBlock)resolve
         reject:(RCTPromiseRejectBlock)reject {
    [self.impl txQuery:txID sql:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)txQueryOne:(NSString *)txID
               sql:(NSString *)sql
        paramsJson:(NSString *)paramsJson
           resolve:(RCTPromiseResolveBlock)resolve
            reject:(RCTPromiseRejectBlock)reject {
    [self.impl txQueryOne:txID sql:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)txExecute:(NSString *)txID
              sql:(NSString *)sql
       paramsJson:(NSString *)paramsJson
          resolve:(RCTPromiseResolveBlock)resolve
           reject:(RCTPromiseRejectBlock)reject {
    [self.impl txExecute:txID sql:sql paramsJson:paramsJson resolve:resolve reject:reject];
}

- (void)commitTransaction:(NSString *)txID
                  resolve:(RCTPromiseResolveBlock)resolve
                   reject:(RCTPromiseRejectBlock)reject {
    [self.impl commitTransaction:txID resolve:resolve reject:reject];
}

- (void)rollbackTransaction:(NSString *)txID
                    resolve:(RCTPromiseResolveBlock)resolve
                     reject:(RCTPromiseRejectBlock)reject {
    [self.impl rollbackTransaction:txID resolve:resolve reject:reject];
}

- (void)createTable:(NSString *)name
        columnsJson:(NSString *)columnsJson
        optionsJson:(NSString *)optionsJson
            resolve:(RCTPromiseResolveBlock)resolve
             reject:(RCTPromiseRejectBlock)reject {
    [self.impl createTable:name columnsJson:columnsJson optionsJson:optionsJson resolve:resolve reject:reject];
}

- (void)alterTable:(NSString *)name
       columnsJson:(NSString *)columnsJson
           resolve:(RCTPromiseResolveBlock)resolve
            reject:(RCTPromiseRejectBlock)reject {
    [self.impl alterTable:name columnsJson:columnsJson resolve:resolve reject:reject];
}

- (void)createIndex:(NSString *)table
            columns:(NSArray *)columns
             unique:(BOOL)unique
            resolve:(RCTPromiseResolveBlock)resolve
             reject:(RCTPromiseRejectBlock)reject {
    [self.impl createIndex:table columns:columns unique:unique resolve:resolve reject:reject];
}

- (void)addChangeObserver:(NSString *)observerID
                   tables:(NSArray *)tables
                  resolve:(RCTPromiseResolveBlock)resolve
                   reject:(RCTPromiseRejectBlock)reject {
    [self.impl addChangeObserver:observerID tables:tables resolve:resolve reject:reject];
}

- (void)addQueryObserver:(NSString *)observerID
                     sql:(NSString *)sql
              paramsJson:(NSString *)paramsJson
                  tables:(NSArray *)tables
                 resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject {
    [self.impl addQueryObserver:observerID sql:sql paramsJson:paramsJson tables:tables resolve:resolve reject:reject];
}

- (void)removeObserver:(NSString *)observerID
               resolve:(RCTPromiseResolveBlock)resolve
                reject:(RCTPromiseRejectBlock)reject {
    [self.impl removeObserver:observerID resolve:resolve reject:reject];
}

- (void)checkpoint:(NSString *)mode
           resolve:(RCTPromiseResolveBlock)resolve
            reject:(RCTPromiseRejectBlock)reject {
    [self.impl checkpoint:mode resolve:resolve reject:reject];
}

- (void)start:(RCTPromiseResolveBlock)resolve
       reject:(RCTPromiseRejectBlock)reject {
    [self.impl start:resolve reject:reject];
}

- (void)stop:(RCTPromiseResolveBlock)resolve
      reject:(RCTPromiseRejectBlock)reject {
    [self.impl stop:resolve reject:reject];
}

- (void)syncNow:(RCTPromiseResolveBlock)resolve
         reject:(RCTPromiseRejectBlock)reject {
    [self.impl syncNow:resolve reject:reject];
}

- (void)pendingChangeCount:(RCTPromiseResolveBlock)resolve
                    reject:(RCTPromiseRejectBlock)reject {
    [self.impl pendingChangeCount:resolve reject:reject];
}

- (void)resolveAuthRequest:(NSString *)requestID
                     token:(NSString *)token {
    [self.impl resolveAuthRequest:requestID token:token];
}

- (void)rejectAuthRequest:(NSString *)requestID
                    error:(NSString *)error {
    [self.impl rejectAuthRequest:requestID error:error];
}

- (void)addListener:(NSString *)eventName {
    // No-op: event delivery handled by Codegen EventEmitter
}

- (void)removeListeners:(double)count {
    // No-op: event delivery handled by Codegen EventEmitter
}

@end

#else

// ──────────────────────────────────────────────────────────────
// Old Architecture fallback: RCT_EXTERN_MODULE bridge
// ──────────────────────────────────────────────────────────────

@interface RCT_EXTERN_MODULE(SynchroModule, RCTEventEmitter)

RCT_EXTERN_METHOD(initialize:(NSDictionary *)config
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(close:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(getPath:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(query:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(queryOne:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(execute:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(executeBatch:(NSString *)statementsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(beginWriteTransaction:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(beginReadTransaction:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(txQuery:(NSString *)txID
                  sql:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(txQueryOne:(NSString *)txID
                  sql:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(txExecute:(NSString *)txID
                  sql:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(commitTransaction:(NSString *)txID
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(rollbackTransaction:(NSString *)txID
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(createTable:(NSString *)name
                  columnsJson:(NSString *)columnsJson
                  optionsJson:(NSString *)optionsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(alterTable:(NSString *)name
                  columnsJson:(NSString *)columnsJson
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(createIndex:(NSString *)table
                  columns:(NSArray<NSString *> *)columns
                  unique:(BOOL)unique
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(addChangeObserver:(NSString *)observerID
                  tables:(NSArray<NSString *> *)tables
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(addQueryObserver:(NSString *)observerID
                  sql:(NSString *)sql
                  paramsJson:(NSString *)paramsJson
                  tables:(NSArray<NSString *> *)tables
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(removeObserver:(NSString *)observerID
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(checkpoint:(NSString *)mode
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(start:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(stop:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(syncNow:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(pendingChangeCount:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(resolveAuthRequest:(NSString *)requestID
                  token:(NSString *)token)
RCT_EXTERN_METHOD(rejectAuthRequest:(NSString *)requestID
                  error:(NSString *)error)
@end

#endif
