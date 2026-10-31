import { WsTicketService } from './ws-ticket.service';

describe('WsTicketService', () => {
  let service: WsTicketService;

  beforeEach(() => {
    service = new WsTicketService();
  });

  it('issues a different ticket each time', () => {
    const a = service.issue('user-1');
    const b = service.issue('user-1');
    expect(a).not.toBe(b);
  });

  it('consumes a freshly issued ticket and returns the userId', () => {
    const ticket = service.issue('user-1');
    expect(service.consume(ticket)).toBe('user-1');
  });

  it('returns null when consuming the same ticket twice (one-time use)', () => {
    const ticket = service.issue('user-1');
    service.consume(ticket);
    expect(service.consume(ticket)).toBeNull();
  });

  it('returns null for an unknown ticket', () => {
    expect(service.consume('does-not-exist')).toBeNull();
  });

  it('returns null for an expired ticket', () => {
    const ticket = service.issue('user-1', -1); // already expired on issue
    expect(service.consume(ticket)).toBeNull();
  });
});
