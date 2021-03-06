package tp.kafka.userprocessor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RichChatMessage {
    private User user;
    private String message;
}