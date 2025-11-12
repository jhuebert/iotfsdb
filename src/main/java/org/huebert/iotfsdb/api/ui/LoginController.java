package org.huebert.iotfsdb.api.ui;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@Slf4j
@Controller
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class LoginController {

    @GetMapping("/ui/login")
    public String getLogin(Model model, 
                           @RequestParam(value = "error", required = false) String error,
                           @RequestParam(value = "logout", required = false) String logout) {
        if (error != null) {
            model.addAttribute("error", true);
        }
        if (logout != null) {
            model.addAttribute("logout", true);
        }
        return "ui/login";
    }

    @PostMapping("/ui/login")
    public String postLogin() {
        // This is handled by Spring Security
        return "redirect:/ui/series";
    }
}
