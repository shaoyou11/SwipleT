#import <Foundation/Foundation.h>

#import "SWFrame.h"

@class SWAccessUnit;
@class SWCancelToken;

extern const NSTimeInterval SWScreenFirstFrameLimit;

@interface SWScreenFeed : NSObject
@property (class, nonatomic, readonly) SWScreenFeed *shared;

- (BOOL)acquire:(SWScreenGeometry *)geometry error:(NSError **)error;
- (void)releaseFeed;
- (void)requestKeyFrame;
- (SWFrame *)latestFrameSequence:(NSInteger *)sequence;
- (SWFrame *)frameAfter:(NSInteger)previous until:(NSDate *)deadline token:(SWCancelToken *)token sequence:(NSInteger *)sequence error:(NSError **)error;
- (NSInteger)addFrameListener:(void (^)(SWFrame *frame))listener;
- (NSInteger)addUnitListener:(void (^)(SWAccessUnit *unit))listener;
- (void)removeListener:(NSInteger)identifier;
- (void)onPackets:(NSData *)batch;
- (void)onEnd:(NSString *)failure;
- (NSString *)diagnosticSummary;
@end

SWFrame *SWScreenFirstFrame(SWScreenFeed *feed, SWCancelToken *token, NSInteger *sequence, NSError **error);
