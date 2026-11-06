//go:build darwin && cgo

#import "SWScreenFeed.h"

#import "SWAccessUnit.h"
#import "SWCancelToken.h"
#import "SWDepacketizer.h"
#import "SWError.h"
#import "SWFrameDecoder.h"
#import "SWGoBridge.h"

const NSTimeInterval SWScreenFirstFrameLimit = 5;

static const NSTimeInterval SWScreenLinger = 1;
static const NSInteger SWPacketHeaderSize = 13;

@implementation SWScreenFeed {
    dispatch_queue_t _queue;
    NSCondition *_state;
    SWDeviceFeed *_feed;
    SWScreenGeometry _geometry;
    NSInteger _users;
    BOOL _opening;
    dispatch_block_t _lingering;
    SWFrame *_latest;
    NSInteger _sequence;
    NSError *_failure;
    NSMutableDictionary<NSNumber *, void (^)(SWFrame *)> *_frameListeners;
    NSMutableDictionary<NSNumber *, void (^)(SWAccessUnit *)> *_unitListeners;
    NSInteger _nextListener;
    SWDepacketizer *_depacketizer;
    SWFrameDecoder *_decoder;
    NSUInteger _diagBatches, _diagRTP, _diagRTCP, _diagUnits, _diagDamaged, _diagKeyframes, _diagRetries, _diagDecoded;
}

+ (SWScreenFeed *)shared {
    static SWScreenFeed *feed;
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        feed = [SWScreenFeed new];
    });
    return feed;
}

- (instancetype)init {
    self = [super init];
    if (self) {
        _queue = dispatch_queue_create("io.nekohasekai.swipt.graphics.screen", dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL, QOS_CLASS_USER_INTERACTIVE, 0));
        _state = [NSCondition new];
        _frameListeners = [NSMutableDictionary dictionary];
        _unitListeners = [NSMutableDictionary dictionary];
        _depacketizer = [SWDepacketizer new];
        _decoder = [SWFrameDecoder new];
        __weak SWScreenFeed *weakSelf = self;
        _decoder.onFrame = ^(CVPixelBufferRef image, SWAccessUnit *unit) {
            [weakSelf decoded:image unit:unit];
        };
    }
    return self;
}

- (BOOL)acquire:(SWScreenGeometry *)geometry error:(NSError **)error {
    [_state lock];
    _users += 1;
    if (_lingering != nil) {
        dispatch_block_cancel(_lingering);
        _lingering = nil;
    }
    while (_opening) {
        [_state wait];
    }
    if (_feed != nil) {
        *geometry = _geometry;
        [_state unlock];
        return YES;
    }
    _opening = YES;
    dispatch_async(_queue, ^{
        [self->_depacketizer reset];
        [self->_decoder invalidate];
    });
    [_state unlock];
    NSError *failure = nil;
    SWDeviceFeed *opened = [SWDeviceFeed openWithError:&failure];
    [_state lock];
    _opening = NO;
    [_state broadcast];
    if (opened == nil) {
        _users -= 1;
        [_state unlock];
        SWFailWith(error, failure);
        return NO;
    }
    _geometry = (SWScreenGeometry){.width = opened.width, .height = opened.height, .scale = opened.scale};
    _feed = opened;
    _failure = nil;
    _latest = nil;
    *geometry = _geometry;
    _diagBatches = _diagRTP = _diagRTCP = _diagUnits = _diagDamaged = _diagKeyframes = _diagRetries = _diagDecoded = 0;
    [_state unlock];
    return YES;
}

- (void)releaseFeed {
    [_state lock];
    _users -= 1;
    if (_users == 0) {
        dispatch_block_t work = dispatch_block_create(0, ^{
            [self closeIfIdle];
        });
        _lingering = work;
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(SWScreenLinger * NSEC_PER_SEC)), dispatch_get_global_queue(QOS_CLASS_UTILITY, 0), work);
    }
    [_state unlock];
}

- (void)closeIfIdle {
    [_state lock];
    SWDeviceFeed *closing = nil;
    if (_users == 0 && _feed != nil) {
        closing = _feed;
        _feed = nil;
        _latest = nil;
    }
    [_state unlock];
    [closing close];
}

- (void)requestKeyFrame {
    [_state lock];
    SWDeviceFeed *feed = _feed;
    [_state unlock];
    [feed requestKeyFrame];
}

- (SWFrame *)latestFrameSequence:(NSInteger *)sequence {
    [_state lock];
    SWFrame *latest = _latest;
    *sequence = _sequence;
    [_state unlock];
    return latest;
}

- (SWFrame *)frameAfter:(NSInteger)previous until:(NSDate *)deadline token:(SWCancelToken *)token sequence:(NSInteger *)sequence error:(NSError **)error {
    [_state lock];
    while (YES) {
        if (_failure != nil) {
            SWFailWith(error, _failure);
            break;
        }
        if (_feed == nil && !_opening) {
            SWFail(error, SWErrorNameUnavailable, @"graphics: the screen stream stopped");
            break;
        }
        if (_latest != nil && _sequence > previous) {
            SWFrame *latest = _latest;
            *sequence = _sequence;
            [_state unlock];
            return latest;
        }
        if (token.isCancelled) {
            SWFailWith(error, SWErrorAborted());
            break;
        }
        NSDate *now = NSDate.date;
        if ([now compare:deadline] != NSOrderedAscending) {
            break;
        }
        [_state waitUntilDate:[deadline earlierDate:[now dateByAddingTimeInterval:0.05]]];
    }
    [_state unlock];
    return nil;
}

- (NSInteger)addFrameListener:(void (^)(SWFrame *))listener {
    [_state lock];
    _nextListener += 1;
    NSInteger identifier = _nextListener;
    _frameListeners[@(identifier)] = listener;
    [_state unlock];
    return identifier;
}

- (NSInteger)addUnitListener:(void (^)(SWAccessUnit *))listener {
    [_state lock];
    _nextListener += 1;
    NSInteger identifier = _nextListener;
    _unitListeners[@(identifier)] = listener;
    [_state unlock];
    return identifier;
}

- (void)removeListener:(NSInteger)identifier {
    [_state lock];
    [_frameListeners removeObjectForKey:@(identifier)];
    [_unitListeners removeObjectForKey:@(identifier)];
    [_state unlock];
}

- (void)onPackets:(NSData *)batch {
    dispatch_async(_queue, ^{
        @autoreleasepool {
            [self process:batch];
        }
    });
}

- (void)onEnd:(NSString *)failure {
    [_state lock];
    _failure = SWErrorFromFailure(failure);
    _feed = nil;
    _latest = nil;
    [_state broadcast];
    [_state unlock];
}

- (void)process:(NSData *)batch {
    [_state lock];
    NSArray<void (^)(SWAccessUnit *)> *listeners = _unitListeners.allValues;
    [_state unlock];
    NSMutableArray<SWAccessUnit *> *units = [NSMutableArray array];
    const uint8_t *bytes = batch.bytes;
    NSInteger total = (NSInteger)batch.length;
    NSInteger offset = 0;
    NSUInteger rtpCount = 0, rtcpCount = 0, damaged = 0, keyframes = 0, retries = 0;
    while (offset + SWPacketHeaderSize <= total) {
        uint64_t arrival = 0;
        for (NSInteger index = 0; index < 8; index++) {
            arrival = arrival << 8 | bytes[offset + index];
        }
        BOOL rtcp = bytes[offset + 8] != 0;
        uint32_t length = 0;
        for (NSInteger index = 9; index < SWPacketHeaderSize; index++) {
            length = length << 8 | bytes[offset + index];
        }
        offset += SWPacketHeaderSize;
        if (offset + (NSInteger)length > total) {
            break;
        }
        if (!rtcp) {
            rtpCount++;
            [_depacketizer push:bytes + offset length:length arrival:(double)arrival / 1e6 into:units];
        } else {
            rtcpCount++;
        }
        offset += length;
    }
    for (SWAccessUnit *unit in units) {
        damaged += unit.damaged ? 1 : 0;
        keyframes += unit.isKeyframe ? 1 : 0;
        for (void (^listener)(SWAccessUnit *) in listeners) {
            listener(unit);
        }
        if ([_decoder decode:unit]) {
            retries++;
            [self requestKeyFrame];
        }
    }
    [_state lock];
    _diagBatches++;
    _diagRTP += rtpCount;
    _diagRTCP += rtcpCount;
    _diagUnits += units.count;
    _diagDamaged += damaged;
    _diagKeyframes += keyframes;
    _diagRetries += retries;
    [_state unlock];
}

- (NSString *)diagnosticSummary {
    [_state lock];
    NSString *summary = [NSString stringWithFormat:@"video-diag batches=%lu rtp=%lu rtcp=%lu units=%lu damaged=%lu keyframes=%lu keyframe_requests=%lu decoded=%lu",
        (unsigned long)_diagBatches, (unsigned long)_diagRTP, (unsigned long)_diagRTCP,
        (unsigned long)_diagUnits, (unsigned long)_diagDamaged, (unsigned long)_diagKeyframes,
        (unsigned long)_diagRetries, (unsigned long)_diagDecoded];
    [_state unlock];
    return summary;
}

- (void)decoded:(CVPixelBufferRef)image unit:(SWAccessUnit *)unit {
    [_state lock];
    if (_feed == nil) {
        [_state unlock];
        return;
    }
    SWFrame *frame = [[SWFrame alloc] initWithPixelBuffer:image time:unit.arrival turns:unit.turns geometry:_geometry];
    _latest = frame;
    _sequence += 1;
    _diagDecoded++;
    [_state broadcast];
    NSArray<void (^)(SWFrame *)> *listeners = _frameListeners.allValues;
    [_state unlock];
    for (void (^listener)(SWFrame *) in listeners) {
        listener(frame);
    }
}

@end

SWFrame *SWScreenFirstFrame(SWScreenFeed *feed, SWCancelToken *token, NSInteger *sequence, NSError **error) {
    SWFrame *latest = [feed latestFrameSequence:sequence];
    if (latest != nil) {
        return latest;
    }
    NSError *failure = nil;
    SWFrame *next = [feed frameAfter:0 until:[NSDate dateWithTimeIntervalSinceNow:SWScreenFirstFrameLimit] token:token sequence:sequence error:&failure];
    if (next != nil) {
        return next;
    }
    if (failure != nil) {
        SWFailWith(error, failure);
    } else {
        SWLogError([feed diagnosticSummary]);
        SWFail(error, SWErrorNameUnavailable, @"graphics: the screen sent no frame within %ld s; it may be off", (long)SWScreenFirstFrameLimit);
    }
    return nil;
}
