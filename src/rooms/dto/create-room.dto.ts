import { IsString, MinLength } from 'class-validator';

export class CreateRoomDto {
  @IsString()
  @MinLength(1)
  name!: string;
}
